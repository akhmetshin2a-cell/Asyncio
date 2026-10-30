"""Дни 1–7. Банковская система, отчёты и визуализация.

Запуск демонстрации: python3 OOP_Project.py
Денежные суммы передавайте строками, целыми числами или Decimal.
Максимальная сумма и стоимость активов одного счёта: 999999999999999999.99.
Денежные операции банковских счетов проходят через обработчик, риск и аудит.
Очередь и журнал рассчитаны на один поток; состояние банка хранится в памяти.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from decimal import Decimal, DecimalException, InvalidOperation, ROUND_HALF_UP, localcontext
from enum import Enum
from uuid import uuid4
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from hashlib import pbkdf2_hmac
from secrets import token_bytes, token_urlsafe, compare_digest
from pathlib import Path
from copy import deepcopy
from tempfile import TemporaryDirectory, NamedTemporaryFile
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import os
import json
import csv


class InvalidOperationError(Exception):
    """Некорректные данные или недопустимая операция со счётом."""


class AccountFrozenError(InvalidOperationError):
    """Операция запрещена: счёт заморожен."""


class AccountClosedError(InvalidOperationError):
    """Операция запрещена: счёт закрыт."""


class InsufficientFundsError(InvalidOperationError):
    """На счёте недостаточно средств."""


MAX_MONEY = Decimal("999999999999999999.99")
_account_access = ContextVar("account_access", default=None)


def money_calculation(method):
    """Точность денежных расчётов не зависит от внешнего Decimal-контекста."""
    @wraps(method)
    def wrapped(*args, **kwargs):
        with localcontext() as context:
            context.prec = 50
            context.rounding = ROUND_HALF_UP
            try:
                return method(*args, **kwargs)
            except DecimalException as error:
                raise InvalidOperationError("Сумма выходит за допустимый диапазон расчётов.") from error
    return wrapped


@contextmanager
def _access_accounts(bank):
    token = _account_access.set(bank._account_token)
    try:
        yield
    finally:
        _account_access.reset(token)


class AccountStatus(Enum):
    ACTIVE = "активный"
    FROZEN = "замороженный"
    CLOSED = "закрытый"


class AbstractAccount(ABC):
    """Общие данные и обязательный интерфейс банковских счетов."""

    def __init__(self, account_id: str, owner: str,
                 balance: Decimal, status: AccountStatus):
        self._account_id = account_id
        self._owner = owner
        self._balance = balance
        self._status = status
        self._bank_token = None

    def _check_mutation_access(self):
        if self._bank_token is not None and _account_access.get() is not self._bank_token:
            raise InvalidOperationError("Используйте методы банка или обработчик транзакций.")

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def balance(self) -> Decimal:
        """Публичное чтение баланса без публичного присваивания."""
        return self._balance

    @property
    def status(self) -> AccountStatus:
        return self._status

    @abstractmethod
    def deposit(self, amount) -> None:
        """Пополнить счёт."""

    @abstractmethod
    def withdraw(self, amount) -> None:
        """Снять средства."""

    @abstractmethod
    def get_account_info(self) -> dict:
        """Вернуть сведения о счёте."""


class BankAccount(AbstractAccount):
    ALLOWED_CURRENCIES = frozenset({"RUB", "USD", "EUR", "KZT", "CNY"})
    # Учебный реестр гарантирует уникальность номеров в пределах процесса.
    _used_ids: set[str] = set()

    def __init__(self, owner: str, balance=0, currency: str = "RUB",
                 status: AccountStatus = AccountStatus.ACTIVE,
                 account_id: str | None = None):
        if not isinstance(owner, str) or not owner.strip():
            raise InvalidOperationError("Имя владельца должно быть непустой строкой.")
        if not isinstance(currency, str):
            raise InvalidOperationError("Валюта должна быть строкой.")
        currency = currency.strip().upper()
        if currency not in self.ALLOWED_CURRENCIES:
            raise InvalidOperationError("Допустимые валюты: RUB, USD, EUR, KZT, CNY.")
        if not isinstance(status, AccountStatus):
            raise InvalidOperationError("Статус должен быть значением AccountStatus.")

        initial_balance = self._validate_amount(balance, allow_zero=True)
        if status is AccountStatus.CLOSED and initial_balance != 0:
            raise InvalidOperationError("Закрытый счёт должен иметь нулевой баланс.")

        if account_id is None:
            account_id = uuid4().hex[:12]
            while account_id in BankAccount._used_ids:
                account_id = uuid4().hex[:12]
        elif not isinstance(account_id, str) or len(account_id.strip()) < 4:
            raise InvalidOperationError("Номер счёта должен содержать минимум 4 символа.")
        account_id = account_id.strip()
        if account_id in BankAccount._used_ids:
            raise InvalidOperationError("Счёт с таким номером уже существует.")

        super().__init__(account_id, owner.strip(), initial_balance, status)
        self._currency = currency
        BankAccount._used_ids.add(account_id)

    @property
    def currency(self) -> str:
        return self._currency

    @staticmethod
    @money_calculation
    def _validate_amount(amount, *, allow_zero: bool = False) -> Decimal:
        # bool — подкласс int, но True и False не считаем денежными суммами.
        if isinstance(amount, bool) or not isinstance(amount, (str, int, Decimal)):
            raise InvalidOperationError("Сумма должна быть строкой, int или Decimal.")
        try:
            value = Decimal(amount)
        except (InvalidOperation, ValueError):
            raise InvalidOperationError("Сумма должна быть корректным числом.") from None
        if not value.is_finite():
            raise InvalidOperationError("Сумма не может быть NaN или бесконечностью.")
        if value < 0 or (value == 0 and not allow_zero):
            raise InvalidOperationError("Сумма должна быть положительной; начальный баланс может быть нулём.")
        if value.as_tuple().exponent < -2:
            raise InvalidOperationError("Допускается не более двух знаков после запятой.")
        if value > MAX_MONEY:
            raise InvalidOperationError(f"Сумма не должна превышать {MAX_MONEY}.")
        return value.quantize(Decimal("0.01"))

    def _check_active(self) -> None:
        if self.status is AccountStatus.FROZEN:
            raise AccountFrozenError("Операция запрещена: счёт заморожен.")
        if self.status is AccountStatus.CLOSED:
            raise AccountClosedError("Операция запрещена: счёт закрыт.")

    @money_calculation
    def deposit(self, amount) -> None:
        self._check_mutation_access()
        self._check_active()
        value = self._validate_amount(amount)
        new_balance = self._balance + value
        # Для инвестиционного счёта ограничивается и общая стоимость активов.
        assets = sum(getattr(self, "_portfolio", {}).values(), Decimal("0"))
        if abs(new_balance) > MAX_MONEY or new_balance + assets > MAX_MONEY:
            raise InvalidOperationError("Превышен максимальный баланс или стоимость активов счёта.")
        self._balance = new_balance

    @money_calculation
    def withdraw(self, amount) -> None:
        self._check_mutation_access()
        self._check_active()
        value = self._validate_amount(amount)
        if value > self.balance:
            raise InsufficientFundsError("Недостаточно средств на счёте.")
        self._balance -= value

    def freeze(self) -> None:
        self._check_mutation_access()
        self._check_active()
        self._status = AccountStatus.FROZEN

    def unfreeze(self) -> None:
        self._check_mutation_access()
        if self.status is AccountStatus.CLOSED:
            raise AccountClosedError("Закрытый счёт нельзя разморозить.")
        if self.status is not AccountStatus.FROZEN:
            raise InvalidOperationError("Разморозить можно только замороженный счёт.")
        self._status = AccountStatus.ACTIVE

    def close(self) -> None:
        self._check_mutation_access()
        self._check_active()
        if self.balance != 0:
            raise InvalidOperationError("Перед закрытием снимите все средства.")
        self._status = AccountStatus.CLOSED

    def get_account_info(self) -> dict:
        return {
            "account_type": type(self).__name__,
            "account_id": self.account_id,
            "owner": self.owner,
            "status": self.status.value,
            "balance": self.balance,
            "currency": self.currency,
        }

    def __str__(self) -> str:
        # UUID содержит буквы: показываем последние четыре символа номера.
        return (f"{type(self).__name__} | Клиент: {self.owner} | "
                f"Счёт: ****{self.account_id[-4:]} | "
                f"Статус: {self.status.value} | "
                f"Баланс: {self.balance:.2f} {self.currency}")


def demonstrate() -> None:
    active = BankAccount("Анна Иванова", balance="1000.00")
    frozen = BankAccount("Иван Петров", balance="500.00",
                         status=AccountStatus.FROZEN)
    print("Созданные счета:")
    print(active)
    print(frozen)

    for operation in (frozen.deposit, frozen.withdraw):
        try:
            operation("100.00")
        except AccountFrozenError as error:
            print(f"{operation.__name__}: {error}")
    assert frozen.balance == Decimal("500.00")

    active.deposit("250.50")
    active.withdraw("100.00")
    assert active.balance == Decimal("1150.50")
    print("После пополнения и снятия:")
    print(active)
    print("Полная информация:", active.get_account_info())

    for amount in ("-10", "0", "abc", True, "NaN", "1.001"):
        try:
            active.deposit(amount)
        except InvalidOperationError as error:
            print(f"Некорректная сумма {amount!r}: {error}")
        else:
            raise AssertionError("Некорректная сумма была принята.")

    try:
        active.withdraw("9999")
    except InsufficientFundsError as error:
        print(error)
    else:
        raise AssertionError("Снятие сверх баланса было разрешено.")
    assert active.balance == Decimal("1150.50")

    active.freeze()
    active.unfreeze()
    active.withdraw(active.balance)
    active.close()
    try:
        active.deposit("10")
    except AccountClosedError as error:
        print(error)
    else:
        raise AssertionError("Закрытый счёт принял пополнение.")
    print(active)


# День 2 — Продвинутые типы банковских счетов.
# Ставки задаются долями единицы: "0.10" означает 10%.


@money_calculation
def validate_rate(rate):
    if isinstance(rate, bool) or not isinstance(rate, (str, int, Decimal)):
        raise InvalidOperationError("Ставка должна быть строкой, int или Decimal.")
    try:
        value = Decimal(rate)
    except (InvalidOperation, ValueError):
        raise InvalidOperationError("Некорректная ставка.") from None
    if not value.is_finite() or not 0 <= value <= 1:
        raise InvalidOperationError("Ставка должна быть от 0 до 1.")
    if value.as_tuple().exponent < -12:
        raise InvalidOperationError("В ставке допускается не более 12 знаков после запятой.")
    return value


class SavingsAccount(BankAccount):
    def __init__(self, owner, balance=0, currency="RUB",
                 status=AccountStatus.ACTIVE, account_id=None,
                 *, min_balance=0, monthly_rate="0.01"):
        minimum = self._validate_amount(min_balance, allow_zero=True)
        initial = self._validate_amount(balance, allow_zero=True)
        rate = validate_rate(monthly_rate)
        if initial < minimum:
            raise InvalidOperationError("Начальный баланс не может быть ниже минимального остатка.")
        super().__init__(owner, initial, currency, status, account_id)
        self._min_balance = minimum
        self._monthly_rate = rate

    @property
    def min_balance(self):
        return self._min_balance

    @property
    def monthly_rate(self):
        return self._monthly_rate

    @money_calculation
    def withdraw(self, amount):
        self._check_mutation_access()
        self._check_active()
        value = self._validate_amount(amount)
        if value > self.balance:
            raise InsufficientFundsError("Недостаточно средств.")
        if self.balance - value < self.min_balance:
            raise InvalidOperationError("Снятие нарушит минимальный остаток.")
        super().withdraw(value)

    @money_calculation
    def apply_monthly_interest(self):
        self._check_mutation_access()
        # Один вызов — один условный месяц, без привязки к календарю.
        self._check_active()
        interest = (self.balance * self.monthly_rate).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP)
        if interest > 0:
            self.deposit(interest)
        return interest

    def get_account_info(self):
        info = super().get_account_info()
        info.update(min_balance=self.min_balance, monthly_rate=self.monthly_rate)
        return info

    def __str__(self):
        return (f"{super().__str__()} | Минимум: {self.min_balance:.2f} | "
                f"Месячная ставка: {self.monthly_rate * 100}%")


class PremiumAccount(BankAccount):
    # Лимит — на одно снятие; фиксированная комиссия — за каждое снятие.
    def __init__(self, owner, balance=0, currency="RUB",
                 status=AccountStatus.ACTIVE, account_id=None,
                 *, withdrawal_limit="1000000", overdraft_limit="50000", fee="100"):
        limit = self._validate_amount(withdrawal_limit)
        overdraft = self._validate_amount(overdraft_limit, allow_zero=True)
        commission = self._validate_amount(fee, allow_zero=True)
        super().__init__(owner, balance, currency, status, account_id)
        self._withdrawal_limit = limit
        self._overdraft_limit = overdraft
        self._fee = commission

    @property
    def withdrawal_limit(self):
        return self._withdrawal_limit

    @property
    def overdraft_limit(self):
        return self._overdraft_limit

    @property
    def fee(self):
        return self._fee

    @money_calculation
    def withdraw(self, amount):
        self._check_mutation_access()
        self._check_active()
        value = self._validate_amount(amount)
        if value > self.withdrawal_limit:
            raise InvalidOperationError("Превышен лимит одного снятия.")
        total = value + self.fee
        if self.balance - total < -self.overdraft_limit:
            raise InsufficientFundsError("Превышен овердрафт с учётом комиссии.")
        # Особое правило: отрицательный баланс допустим в пределах овердрафта.
        self._balance -= total

    def get_account_info(self):
        info = super().get_account_info()
        info.update(withdrawal_limit=self.withdrawal_limit,
                    overdraft_limit=self.overdraft_limit, fee=self.fee)
        return info

    def __str__(self):
        return (f"{super().__str__()} | Лимит снятия: {self.withdrawal_limit:.2f} | "
                f"Овердрафт: {self.overdraft_limit:.2f} | Комиссия: {self.fee:.2f}")


class InvestmentAccount(BankAccount):
    ALLOWED_ASSETS = frozenset({"stocks", "bonds", "etf"})

    def __init__(self, owner, balance=0, currency="RUB",
                 status=AccountStatus.ACTIVE, account_id=None):
        super().__init__(owner, balance, currency, status, account_id)
        self._portfolio = {asset: Decimal("0") for asset in sorted(self.ALLOWED_ASSETS)}

    @property
    def portfolio(self):
        return self._portfolio.copy()

    @money_calculation
    def invest(self, asset_type, amount):
        self._check_mutation_access()
        self._check_active()
        if not isinstance(asset_type, str) or asset_type not in self.ALLOWED_ASSETS:
            raise InvalidOperationError("Допустимые активы: stocks, bonds, etf.")
        value = self._validate_amount(amount)
        if value > self.balance:
            raise InsufficientFundsError("Недостаточно свободных средств.")
        self._balance -= value
        self._portfolio[asset_type] += value

    @money_calculation
    def withdraw(self, amount):
        self._check_mutation_access()
        self._check_active()
        value = self._validate_amount(amount)
        if value > self.balance:
            raise InsufficientFundsError("Снять можно только свободные деньги.")
        super().withdraw(value)

    @money_calculation
    def project_yearly_growth(self, growth_rates):
        # Это прогноз прироста, а не начисление денег на счёт.
        if not isinstance(growth_rates, dict):
            raise InvalidOperationError("Ставки должны быть словарём.")
        rates = {}
        for asset, rate in growth_rates.items():
            if asset not in self.ALLOWED_ASSETS:
                raise InvalidOperationError(f"Неизвестный актив: {asset}.")
            rates[asset] = validate_rate(rate)
        growth = Decimal("0")
        for asset, amount in self._portfolio.items():
            if amount > 0:
                if asset not in rates:
                    raise InvalidOperationError(f"Не указана ставка для {asset}.")
                growth += amount * rates[asset]
        return growth.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def close(self):
        self._check_mutation_access()
        self._check_active()
        if any(self._portfolio.values()):
            raise InvalidOperationError("Нельзя закрыть счёт с непустым портфелем.")
        super().close()

    @money_calculation
    def get_account_info(self):
        info = super().get_account_info()
        invested = sum(self._portfolio.values(), Decimal("0"))
        info.update(portfolio=self.portfolio, invested_total=invested,
                    total_assets=self.balance + invested)
        return info

    def __str__(self):
        assets = ", ".join(f"{asset}: {amount:.2f}" for asset, amount in self._portfolio.items())
        return f"{super().__str__()} | Портфель ({self.currency}): {assets}"


def expect_error(error_type, operation, *args):
    try:
        operation(*args)
    except error_type as error:
        print(f"Ожидаемая ошибка: {error}")
    else:
        raise AssertionError(f"Ожидалась ошибка {error_type.__name__}")


def demonstrate_day2():
    savings = SavingsAccount("Анна", "1000", min_balance="500")
    savings_frozen = SavingsAccount("Борис", "2000", status=AccountStatus.FROZEN)
    premium = PremiumAccount("Вера", "1000", overdraft_limit="500", fee="50")
    premium_second = PremiumAccount("Глеб", "10000", fee="10", withdrawal_limit="2000")
    investment = InvestmentAccount("Дарья", "10000")
    investment_second = InvestmentAccount("Егор", "5000", currency="EUR")

    savings.deposit("200")
    savings.withdraw("300")
    assert savings.apply_monthly_interest() == Decimal("9.00")
    expect_error(InvalidOperationError, savings.withdraw, "410")
    expect_error(AccountFrozenError, savings_frozen.apply_monthly_interest)
    assert savings.balance == Decimal("909.00")

    premium.withdraw("1400")
    assert premium.balance == Decimal("-450")
    expect_error(InsufficientFundsError, premium.withdraw, "1")
    assert premium.balance == Decimal("-450")
    premium.deposit("450")
    premium.close()
    expect_error(AccountClosedError, premium.withdraw, "1")
    expect_error(InvalidOperationError, premium_second.withdraw, "2001")
    premium_second.withdraw("100")
    assert premium_second.balance == Decimal("9890")

    investment.invest("stocks", "3000")
    investment.invest("bonds", "2000")
    investment.invest("etf", "1000")
    rates = {"stocks": "0.10", "bonds": "0.04", "etf": "0.07"}
    growth = investment.project_yearly_growth(rates)
    assert growth == Decimal("450.00")
    print(f"Ожидаемый годовой прирост: {growth:.2f} RUB")
    expect_error(InvalidOperationError, investment.invest, "crypto", "100")
    expect_error(InvalidOperationError, investment.invest, "stocks", "-1")
    expect_error(InsufficientFundsError, investment.invest, "stocks", "4001")
    expect_error(InsufficientFundsError, investment.withdraw, "4001")
    expect_error(InvalidOperationError, investment.project_yearly_growth, {})
    assert investment.balance == Decimal("4000")
    assert investment.portfolio["stocks"] == Decimal("3000")
    investment.withdraw("4000")
    expect_error(InvalidOperationError, investment.close)
    investment_second.invest("etf", "1000")
    assert investment_second.project_yearly_growth(rates) == Decimal("70.00")

    print("\nПолиморфизм: одни методы, разные типы счетов")
    for account in (savings, savings_frozen, premium, premium_second,
                    investment, investment_second):
        print(account)
        print(account.get_account_info())
    for account in (savings, premium_second, investment_second):
        account.withdraw("100")
    assert savings.balance == Decimal("809.00")
    assert premium_second.balance == Decimal("9780")
    assert investment_second.balance == Decimal("3900")
    print("Все проверки второго дня пройдены.")


# День 3 — Клиенты, управление счетами и безопасность.


class AuthenticationError(InvalidOperationError):
    pass


class ClientBlockedError(AuthenticationError):
    pass


class NightOperationError(InvalidOperationError):
    pass


class ClientStatus(Enum):
    ACTIVE = "активный"
    BLOCKED = "заблокированный"


class Client:
    """Возраст задаётся на момент регистрации; данные хранятся в памяти."""
    def __init__(self, full_name, age, contacts, client_id=None):
        if not isinstance(full_name, str) or not full_name.strip():
            raise InvalidOperationError("ФИО должно быть непустой строкой.")
        if isinstance(age, bool) or not isinstance(age, int) or age < 18:
            raise InvalidOperationError("Клиент должен быть совершеннолетним (возраст >= 18).")
        if not isinstance(contacts, dict) or not contacts:
            raise InvalidOperationError("Укажите контакты в непустом словаре.")
        for key, value in contacts.items():
            if not isinstance(key, str) or not key.strip() or not isinstance(value, str) or not value.strip():
                raise InvalidOperationError("Названия и значения контактов должны быть непустыми строками.")
        if client_id is not None and (not isinstance(client_id, str) or not client_id.strip()):
            raise InvalidOperationError("ID клиента должен быть непустой строкой.")
        self._client_id = client_id.strip() if client_id is not None else uuid4().hex
        self._full_name = full_name.strip()
        self._age = age
        self._contacts = {key.strip(): value.strip() for key, value in contacts.items()}
        self._status = ClientStatus.ACTIVE
        self._account_ids = []

    @property
    def client_id(self):
        return self._client_id

    @property
    def full_name(self):
        return self._full_name

    @property
    def status(self):
        return self._status

    @property
    def contacts(self):
        return self._contacts.copy()

    @property
    def account_ids(self):
        # В списке остаются и номера закрытых счетов, как история клиента.
        return self._account_ids.copy()

    def __str__(self):
        return f"Client | {self.full_name} | ID: {self.client_id} | {self.status.value}"


class Bank:
    """Учебный банк. Ночная проверка применяется к изменяющим операциям.

    Вход, поиск и чтение доступны ночью. Часы по умолчанию — Europe/Moscow.
    Для тестирования можно передать функцию clock, возвращающую datetime.
    Сессии действуют до выхода или блокировки; данные не сохраняются на диск.
    """
    ACCOUNT_TYPES = {
        "bank": BankAccount,
        "savings": SavingsAccount,
        "premium": PremiumAccount,
        "investment": InvestmentAccount,
    }

    def __init__(self, name, clock=None, *, audit=None, risk_analyzer=None):
        if not isinstance(name, str) or not name.strip():
            raise InvalidOperationError("Название банка должно быть непустым.")
        self.name = name.strip()
        self._clock = clock if clock is not None else lambda: datetime.now(ZoneInfo("Europe/Moscow"))
        self._clients = {}
        self._accounts = {}
        self._owners = {}
        self._credentials = {}
        self._failed_attempts = {}
        self._sessions = {}
        self._security_events = []
        self._account_token = object()
        self._completed_transactions = {}
        self._transactions = {}
        self._processor = None
        self._audit = audit if audit is not None else (
            risk_analyzer.audit if risk_analyzer is not None else AuditLog(clock=self._clock))
        self._risk_analyzer = risk_analyzer or RiskAnalyzer(self._audit, clock=self._clock)
        if not isinstance(self._audit, AuditLog) or not isinstance(self._risk_analyzer, RiskAnalyzer):
            raise InvalidOperationError("Нужны AuditLog и RiskAnalyzer.")
        if self._risk_analyzer.audit is not self._audit:
            raise InvalidOperationError("Банк и анализатор должны использовать один аудит.")

    @property
    def audit(self):
        return self._audit

    @property
    def risk_analyzer(self):
        return self._risk_analyzer

    @property
    def transactions(self):
        return tuple(self._transactions.values())

    def _get_processor(self):
        if self._processor is None:
            TransactionProcessor(self, TransactionQueue(self._clock), clock=self._clock)
        return self._processor

    def _perform_transaction(self, session, account_id, kind, amount, **settings):
        _, account = self._owned_account(session, account_id, kind.value)
        transaction = Transaction(kind, amount, account.currency, session=session,
                                  clock=self._clock, **settings)
        self._get_processor().execute_now(transaction)
        if transaction.status is not TransactionStatus.COMPLETED:
            raise transaction._last_error
        return transaction

    @property
    def security_events(self):
        return [event.copy() for event in self._security_events]

    def _flag(self, client_id, action, reason):
        self._security_events.append({"time": self._clock().isoformat(),
                                      "client_id": client_id,
                                      "action": action, "reason": reason})

    def _check_daytime(self, client_id, action):
        if 0 <= self._clock().hour < 5:
            self._flag(client_id, action, "Попытка операции в запрещённое ночное время")
            raise NightOperationError("Операции запрещены с 00:00 до 05:00.")

    def _get_client(self, client_id):
        if not isinstance(client_id, str) or client_id not in self._clients:
            raise AuthenticationError("Клиент не найден.")
        return self._clients[client_id]

    def _authorize(self, session):
        if not isinstance(session, str) or session not in self._sessions:
            self._flag(None, "authorization", "Недействительная сессия")
            raise AuthenticationError("Сначала выполните вход.")
        client = self._get_client(self._sessions[session])
        if client.status is ClientStatus.BLOCKED:
            raise ClientBlockedError("Клиент заблокирован.")
        return client

    def _owned_account(self, session, account_id, action):
        client = self._authorize(session)
        if not isinstance(account_id, str) or self._owners.get(account_id) != client.client_id:
            self._flag(client.client_id, action, "Попытка доступа к чужому или неизвестному счёту")
            raise InvalidOperationError("Счёт не принадлежит клиенту или не найден.")
        return client, self._accounts[account_id]

    def add_client(self, client, password):
        # Регистрация — операция управляющего кода, а не клиента с сессией.
        if not isinstance(client, Client):
            raise InvalidOperationError("Передайте объект Client.")
        self._check_daytime(client.client_id, "add_client")
        if client.client_id in self._clients:
            raise InvalidOperationError("Клиент с таким ID уже зарегистрирован.")
        if client.status is not ClientStatus.ACTIVE or client.account_ids:
            raise InvalidOperationError("Для регистрации нужен новый активный клиент без счетов.")
        if not isinstance(password, str) or not password:
            raise InvalidOperationError("Пароль должен быть непустой строкой.")
        salt = token_bytes(16)
        digest = pbkdf2_hmac("sha256", password.encode(), salt, 200000)
        self._credentials[client.client_id] = (salt, digest)
        self._clients[client.client_id] = client
        self._failed_attempts[client.client_id] = 0
        return client.client_id

    def authenticate_client(self, client_id, password):
        client = self._get_client(client_id)
        if client.status is ClientStatus.BLOCKED:
            self._flag(client_id, "login", "Вход заблокированного клиента")
            raise ClientBlockedError("Клиент заблокирован после трёх неверных попыток.")
        salt, expected = self._credentials[client_id]
        valid = isinstance(password, str) and compare_digest(
            pbkdf2_hmac("sha256", password.encode(), salt, 200000), expected)
        if not valid:
            self._failed_attempts[client_id] += 1
            self._flag(client_id, "login", "Неверный пароль")
            if self._failed_attempts[client_id] >= 3:
                client._status = ClientStatus.BLOCKED
                self._sessions = {key: owner for key, owner in self._sessions.items() if owner != client_id}
                raise ClientBlockedError("Три неверные попытки: клиент заблокирован.")
            raise AuthenticationError("Неверный пароль.")
        self._failed_attempts[client_id] = 0
        session = token_urlsafe(32)
        self._sessions[session] = client_id
        return session

    def logout(self, session):
        self._authorize(session)
        del self._sessions[session]

    def open_account(self, session, account_type="bank", **settings):
        client = self._authorize(session)
        self._check_daytime(client.client_id, "open_account")
        if not isinstance(account_type, str) or account_type not in self.ACCOUNT_TYPES:
            raise InvalidOperationError("Тип счёта: bank, savings, premium или investment.")
        if "owner" in settings or "status" in settings:
            raise InvalidOperationError("Владелец назначается банком, новый счёт всегда активен.")
        # Ошибки в названиях параметров превращаем в ошибку модели.
        try:
            account = self.ACCOUNT_TYPES[account_type](owner=client.full_name, **settings)
        except TypeError as error:
            raise InvalidOperationError(f"Неверные параметры счёта: {error}") from None
        self._accounts[account.account_id] = account
        account._bank_token = self._account_token
        self._owners[account.account_id] = client.client_id
        client._account_ids.append(account.account_id)
        return account.account_id

    def _change_account(self, session, account_id, method, *args):
        client, account = self._owned_account(session, account_id, method)
        self._check_daytime(client.client_id, method)
        with _access_accounts(self):
            return getattr(account, method)(*args)

    def close_account(self, session, account_id):
        return self._change_account(session, account_id, "close")

    def freeze_account(self, session, account_id):
        return self._change_account(session, account_id, "freeze")

    def unfreeze_account(self, session, account_id):
        return self._change_account(session, account_id, "unfreeze")

    def deposit(self, session, account_id, amount):
        self._perform_transaction(session, account_id, TransactionType.DEPOSIT, amount,
                                  recipient=account_id)

    def withdraw(self, session, account_id, amount):
        self._perform_transaction(session, account_id, TransactionType.WITHDRAWAL, amount,
                                  sender=account_id)

    def invest(self, session, account_id, asset_type, amount):
        _, account = self._owned_account(session, account_id, "invest")
        if not isinstance(account, InvestmentAccount):
            raise InvalidOperationError("Инвестировать можно только с инвестиционного счёта.")
        self._perform_transaction(session, account_id, TransactionType.INVESTMENT, amount,
                                  sender=account_id, asset_type=asset_type)

    @money_calculation
    def apply_monthly_interest(self, session, account_id):
        _, account = self._owned_account(session, account_id, "interest")
        if not isinstance(account, SavingsAccount):
            raise InvalidOperationError("Месячные проценты доступны накопительному счёту.")
        self._check_daytime(self._sessions[session], "interest")
        account._check_active()
        amount = (account.balance * account.monthly_rate).quantize(Decimal("0.01"))
        if amount == 0:
            return amount
        transaction = self._perform_transaction(
            session, account_id, TransactionType.INTEREST, amount, recipient=account_id)
        return transaction.recipient_amount

    def search_accounts(self, session, *, account_id=None, currency=None, status=None):
        client = self._authorize(session)
        if currency is not None and currency not in BankAccount.ALLOWED_CURRENCIES:
            raise InvalidOperationError("Неизвестная валюта.")
        if status is not None and not isinstance(status, AccountStatus):
            raise InvalidOperationError("Укажите статус AccountStatus.")
        result = []
        for number in client.account_ids:
            account = self._accounts[number]
            if account_id is not None and account.account_id != account_id:
                continue
            if currency is not None and account.currency != currency:
                continue
            if status is not None and account.status is not status:
                continue
            result.append(account.get_account_info())
        return result

    @money_calculation
    def get_total_balance(self):
        # Управленческая сводка: валюты не складываются между собой.
        # Включены замороженные счета, овердрафт вычитается, портфель учитывается.
        totals = {currency: Decimal("0") for currency in sorted(BankAccount.ALLOWED_CURRENCIES)}
        for account in self._accounts.values():
            if account.status is not AccountStatus.CLOSED:
                info = account.get_account_info()
                totals[account.currency] += info.get("total_assets", account.balance)
        return totals

    @money_calculation
    def get_clients_ranking(self):
        # Отдельный рейтинг для каждой валюты, без выдуманного обменного курса.
        ranking = {}
        for currency in sorted(BankAccount.ALLOWED_CURRENCIES):
            rows = []
            for client in self._clients.values():
                accounts = [self._accounts[number] for number in client.account_ids
                            if self._accounts[number].currency == currency
                            and self._accounts[number].status is not AccountStatus.CLOSED]
                if accounts:
                    total = sum((account.get_account_info().get("total_assets", account.balance)
                                 for account in accounts), Decimal("0"))
                    rows.append({"client_id": client.client_id, "full_name": client.full_name,
                                 "total_balance": total})
            ranking[currency] = sorted(rows, key=lambda row: (-row["total_balance"], row["client_id"]))
        return ranking


def demonstrate_day3():
    print("\nДЕНЬ 3 — СИСТЕМА BANK")
    current = [datetime(2026, 10, 12, 12, 0)]
    bank = Bank("Учебный банк", clock=lambda: current[0])
    anna = Client("Анна Иванова", 25, {"phone": "+70000000001"})
    boris = Client("Борис Петров", 18, {"email": "boris@example.test"})
    vera = Client("Вера Сидорова", 30, {"phone": "+70000000002"})
    expect_error(InvalidOperationError, Client, "Несовершеннолетний", 17, {"phone": "123"})
    for client in (anna, boris, vera):
        bank.add_client(client, "учебный-пароль")
    expect_error(InvalidOperationError, bank.add_client, anna, "пароль")
    anna_session = bank.authenticate_client(anna.client_id, "учебный-пароль")
    boris_session = bank.authenticate_client(boris.client_id, "учебный-пароль")
    vera_session = bank.authenticate_client(vera.client_id, "учебный-пароль")
    anna_account = bank.open_account(anna_session, "bank", balance="1000")
    anna_invest = bank.open_account(anna_session, "investment", balance="500")
    boris_account = bank.open_account(boris_session, "savings", balance="2000", min_balance="500")
    bank.open_account(vera_session, "premium", balance="300", currency="USD")
    bank.invest(anna_session, anna_invest, "stocks", "200")
    bank.deposit(anna_session, anna_account, "100")
    bank.withdraw(anna_session, anna_account, "50")
    bank.freeze_account(anna_session, anna_account)
    expect_error(AccountFrozenError, bank.withdraw, anna_session, anna_account, "1")
    bank.unfreeze_account(anna_session, anna_account)
    expect_error(InvalidOperationError, bank.withdraw, boris_session, anna_account, "1")
    expect_error(InvalidOperationError, bank.close_account, anna_session, anna_account)
    expect_error(AuthenticationError, bank.authenticate_client, vera.client_id, "неверно")
    expect_error(AuthenticationError, bank.authenticate_client, vera.client_id, "неверно")
    expect_error(ClientBlockedError, bank.authenticate_client, vera.client_id, "неверно")
    expect_error(ClientBlockedError, bank.authenticate_client, vera.client_id, "учебный-пароль")
    expect_error(AuthenticationError, bank.open_account, vera_session)
    assert vera.status is ClientStatus.BLOCKED
    current[0] = datetime(2026, 10, 13, 0, 0)
    expect_error(RiskBlockedError, bank.deposit, anna_session, anna_account, "10")
    current[0] = datetime(2026, 10, 13, 4, 59)
    expect_error(NightOperationError, bank.open_account, anna_session)
    assert bank.search_accounts(anna_session, account_id=anna_account)[0]["balance"] == Decimal("1050")
    current[0] = datetime(2026, 10, 13, 5, 0)
    bank.deposit(anna_session, anna_account, "10")
    assert bank.get_total_balance()["RUB"] == Decimal("3560")
    assert bank.get_total_balance()["USD"] == Decimal("300")
    assert bank.get_clients_ranking()["RUB"][0]["client_id"] == boris.client_id
    assert len(bank.search_accounts(anna_session)) == 2
    assert len(bank.search_accounts(boris_session, currency="RUB")) == 1
    assert bank.apply_monthly_interest(boris_session, boris_account) == Decimal("20.00")
    bank.withdraw(anna_session, anna_account, "1060")
    bank.close_account(anna_session, anna_account)
    expect_error(AccountClosedError, bank.deposit, anna_session, anna_account, "1")
    assert bank.get_total_balance()["RUB"] == Decimal("2520")
    print("Общие средства по валютам:", bank.get_total_balance())
    print("Рейтинг клиентов:", bank.get_clients_ranking())
    print("Журнал подозрительных действий:")
    for event in bank.security_events:
        print(event)
    bank.logout(anna_session)
    expect_error(AuthenticationError, bank.search_accounts, anna_session)
    print("Все проверки третьего дня пройдены.")


# День 4 — Транзакции и обработка очереди.
# Внешний перевод в этой модели только списывает деньги и фиксирует результат:
# реальной отправки во внешний банк нет. Курсы — условные, не рыночные.


class TransactionType(Enum):
    TRANSFER = "внутренний перевод"
    EXTERNAL_TRANSFER = "внешний перевод"
    DEPOSIT = "пополнение"
    WITHDRAWAL = "снятие"
    INVESTMENT = "инвестирование"
    INTEREST = "начисление процентов"


class TransactionStatus(Enum):
    CREATED = "создана"
    QUEUED = "в очереди"
    PROCESSING = "обрабатывается"
    RETRY = "ожидает повторной попытки"
    COMPLETED = "выполнена"
    FAILED = "отклонена"
    CANCELLED = "отменена"


class TemporaryTransactionError(Exception):
    """Временный сбой: операция может быть повторена."""


class Transaction:
    _TRANSITIONS = {
        TransactionStatus.CREATED: {TransactionStatus.QUEUED},
        TransactionStatus.QUEUED: {TransactionStatus.PROCESSING, TransactionStatus.CANCELLED},
        TransactionStatus.RETRY: {TransactionStatus.PROCESSING, TransactionStatus.CANCELLED},
        TransactionStatus.PROCESSING: {TransactionStatus.RETRY, TransactionStatus.COMPLETED,
                                       TransactionStatus.FAILED},
    }

    def __setattr__(self, name, value):
        if getattr(self, "_sealed", False) and not name.startswith("_"):
            raise AttributeError(f"Поле транзакции {name} доступно только для чтения.")
        object.__setattr__(self, name, value)

    def _update(self, **changes):
        for name, value in changes.items():
            object.__setattr__(self, name, value)

    def _transition(self, status, now, **changes):
        if status not in self._TRANSITIONS.get(self.status, set()):
            raise InvalidOperationError("Недопустимый переход статуса транзакции.")
        self._update(status=status, updated_at=now, **changes)

    @property
    def errors(self):
        return deepcopy(self._errors)

    def __init__(self, transaction_type, amount, currency, *, sender=None,
                 recipient=None, session=None, priority=0, scheduled_at=None, clock=None,
                 asset_type=None):
        if not isinstance(transaction_type, TransactionType):
            raise InvalidOperationError("Тип должен быть TransactionType.")
        if not isinstance(currency, str) or currency.upper() not in BankAccount.ALLOWED_CURRENCIES:
            raise InvalidOperationError("Неизвестная валюта транзакции.")
        if isinstance(priority, bool) or not isinstance(priority, int):
            raise InvalidOperationError("Приоритет должен быть целым числом.")
        self._clock = clock or (lambda: datetime.now(ZoneInfo("Europe/Moscow")))
        now = self._clock()
        if scheduled_at is not None:
            if not isinstance(scheduled_at, datetime):
                raise InvalidOperationError("Время исполнения должно быть datetime.")
            if (scheduled_at.utcoffset() is None) != (now.utcoffset() is None):
                raise InvalidOperationError("Время операции и часы должны иметь совместимые часовые пояса.")
        if transaction_type in (TransactionType.TRANSFER, TransactionType.EXTERNAL_TRANSFER,
                                TransactionType.WITHDRAWAL, TransactionType.INVESTMENT):
            if not isinstance(sender, str) or not sender.strip():
                raise InvalidOperationError("Укажите номер счёта отправителя.")
        if transaction_type in (TransactionType.TRANSFER, TransactionType.EXTERNAL_TRANSFER,
                                TransactionType.DEPOSIT, TransactionType.INTEREST):
            if not isinstance(recipient, str) or not recipient.strip():
                raise InvalidOperationError("Укажите получателя.")
        if transaction_type is TransactionType.TRANSFER and sender == recipient:
            raise InvalidOperationError("Нельзя переводить на тот же счёт.")
        if transaction_type is TransactionType.INVESTMENT and asset_type not in InvestmentAccount.ALLOWED_ASSETS:
            raise InvalidOperationError("Неизвестный тип актива.")
        self.transaction_id = uuid4().hex
        self.type = transaction_type
        self.amount = BankAccount._validate_amount(amount)
        self.currency = currency.upper()
        self.fee = Decimal("0")
        self.recipient_amount = Decimal("0")
        self.sender = sender
        self.recipient = recipient
        self.asset_type = asset_type
        self._session = session
        self.priority = priority
        self.status = TransactionStatus.CREATED
        self.failure_reason = None
        self.created_at = now
        self.updated_at = now
        self.scheduled_at = scheduled_at if scheduled_at is not None else now
        self.completed_at = None
        self.attempts = 0
        self._errors = []
        self._last_error = None
        self._sealed = True

    def _validate_for_execution(self):
        if not isinstance(self.type, TransactionType):
            raise InvalidOperationError("Неизвестный тип транзакции.")
        BankAccount._validate_amount(self.amount)
        if self.currency not in BankAccount.ALLOWED_CURRENCIES:
            raise InvalidOperationError("Неизвестная валюта транзакции.")
        if self.type is TransactionType.TRANSFER and self.sender == self.recipient:
            raise InvalidOperationError("Нельзя переводить на тот же счёт.")

    def __str__(self):
        return (f"Transaction {self.transaction_id[:8]} | {self.type.value} | "
                f"{self.amount:.2f} {self.currency} | комиссия: {self.fee:.2f} | "
                f"{self.status.value} | попыток: {self.attempts} | "
                f"причина: {self.failure_reason or '—'}")


class TransactionQueue:
    """Больше priority — раньше исполнение; при равенстве порядок добавления.

    Будущая операция не мешает готовым. Одна транзакция добавляется один раз.
    Обработка рассчитана на один поток, без базы данных.
    """
    def __init__(self, clock=None):
        self._clock = clock or (lambda: datetime.now(ZoneInfo("Europe/Moscow")))
        self._items = {}
        self._sequence = 0

    def add(self, transaction):
        if not isinstance(transaction, Transaction):
            raise InvalidOperationError("Передайте Transaction.")
        if transaction.status is not TransactionStatus.CREATED or transaction.transaction_id in self._items:
            raise InvalidOperationError("Транзакция уже добавлена или завершена.")
        now = self._clock()
        if (transaction.scheduled_at.utcoffset() is None) != (now.utcoffset() is None):
            raise InvalidOperationError("Несовместимые часовые пояса очереди и транзакции.")
        self._items[transaction.transaction_id] = (self._sequence, transaction)
        self._sequence += 1
        transaction._transition(TransactionStatus.QUEUED, now)

    def cancel(self, transaction_id):
        item = self._items.get(transaction_id)
        if item is None:
            raise InvalidOperationError("Транзакция не найдена в очереди.")
        transaction = item[1]
        if transaction.status not in (TransactionStatus.QUEUED, TransactionStatus.RETRY):
            raise InvalidOperationError("Отменить можно только ожидающую транзакцию.")
        transaction._transition(TransactionStatus.CANCELLED, self._clock(),
                                failure_reason="Отменена до исполнения")

    def ready(self):
        now = self._clock()
        items = [item for item in self._items.values()
                 if item[1].status in (TransactionStatus.QUEUED, TransactionStatus.RETRY)
                 and item[1].scheduled_at <= now]
        return [item[1] for item in sorted(items, key=lambda item: (-item[1].priority, item[0]))]


class TransactionProcessor:
    def __init__(self, bank, queue, *, currency_values=None, external_fee_rate="0.01",
                 max_attempts=3, retry_delay=60, clock=None, before_execute=None,
                 audit=None, risk_analyzer=None):
        if not isinstance(bank, Bank) or not isinstance(queue, TransactionQueue):
            raise InvalidOperationError("Нужны Bank и TransactionQueue.")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts < 1:
            raise InvalidOperationError("Число попыток должно быть положительным целым.")
        if isinstance(retry_delay, bool) or not isinstance(retry_delay, int) or retry_delay < 1:
            raise InvalidOperationError("Задержка повтора должна быть положительной в секундах.")
        self.bank = bank
        self.queue = queue
        self._clock = clock or queue._clock
        self.external_fee_rate = validate_rate(external_fee_rate)
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        # Стоимость одной единицы валюты в условных RUB.
        values = currency_values if currency_values is not None else {
            "RUB": "1", "USD": "100", "EUR": "110", "KZT": "0.20", "CNY": "14"}
        if not isinstance(values, dict) or set(values) != set(BankAccount.ALLOWED_CURRENCIES):
            raise InvalidOperationError("Задайте стоимость всех пяти валют.")
        self.currency_values = {currency: BankAccount._validate_amount(value)
                                for currency, value in values.items()}
        self.before_execute = before_execute
        if audit is not None and not isinstance(audit, AuditLog):
            raise InvalidOperationError("Передайте AuditLog.")
        if risk_analyzer is not None and not isinstance(risk_analyzer, RiskAnalyzer):
            raise InvalidOperationError("Передайте RiskAnalyzer.")
        self.audit = audit if audit is not None else (
            risk_analyzer.audit if risk_analyzer is not None else bank.audit)
        self.risk_analyzer = risk_analyzer or (
            bank.risk_analyzer if self.audit is bank.audit else RiskAnalyzer(self.audit, clock=self._clock))
        if self.audit is not self.risk_analyzer.audit:
            raise InvalidOperationError("Анализатор и обработчик должны использовать один аудит.")
        self.error_log = []
        self.execution_order = []
        bank._audit = self.audit
        bank._risk_analyzer = self.risk_analyzer
        bank._processor = self

    @money_calculation
    def convert(self, amount, source, target):
        return (amount * self.currency_values[source] / self.currency_values[target]).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP)

    @money_calculation
    def _execute(self, transaction):
        t = transaction
        t._validate_for_execution()
        if t.transaction_id in self.bank._completed_transactions:
            raise InvalidOperationError("Транзакция уже проведена.")
        client = self.bank._authorize(t._session)
        incoming = t.type in (TransactionType.DEPOSIT, TransactionType.INTEREST)
        _, account = self.bank._owned_account(
            t._session, t.recipient if incoming else t.sender, "transaction")
        account._check_active()
        if account.currency != t.currency:
            raise InvalidOperationError("Сумма и комиссия задаются в валюте отправителя.")
        amount_rub = self.convert(t.amount, t.currency, "RUB")
        level, assessment = self.risk_analyzer.assess(t, client.client_id, amount_rub)
        if level is RiskLevel.HIGH:
            self.bank._flag(client.client_id, "transaction", "; ".join(assessment["reasons"]))
            raise RiskBlockedError("Высокий риск: " + "; ".join(assessment["reasons"]))
        self.bank._check_daytime(client.client_id, "transaction")
        if incoming:
            if t.type is TransactionType.INTEREST:
                if not isinstance(account, SavingsAccount):
                    raise InvalidOperationError("Начисление процентов доступно накопительному счёту.")
                expected = (account.balance * account.monthly_rate).quantize(Decimal("0.01"))
                if t.amount != expected:
                    raise InvalidOperationError("Сумма процентов не соответствует текущему балансу и ставке.")
            with _access_accounts(self.bank):
                account.deposit(t.amount)
            t._update(recipient_amount=t.amount)
            return
        if t.type is TransactionType.INVESTMENT:
            if not isinstance(account, InvestmentAccount):
                raise InvalidOperationError("Нужен инвестиционный счёт.")
            with _access_accounts(self.bank):
                account.invest(t.asset_type, t.amount)
            return
        if t.type not in (TransactionType.WITHDRAWAL, TransactionType.TRANSFER,
                           TransactionType.EXTERNAL_TRANSFER):
            raise InvalidOperationError("Неизвестный тип транзакции.")
        sender = account
        if sender.balance < 0 and not isinstance(sender, PremiumAccount):
            raise InvalidOperationError("Перевод с отрицательного баланса запрещён.")
        recipient = None
        converted = Decimal("0")
        if t.type is TransactionType.TRANSFER:
            recipient = self.bank._accounts.get(t.recipient)
            if recipient is None:
                raise InvalidOperationError("Счёт получателя не найден.")
            recipient._check_active()
            owner = self.bank._get_client(self.bank._owners[t.recipient])
            if owner.status is ClientStatus.BLOCKED:
                raise ClientBlockedError("Получатель заблокирован.")
            converted = self.convert(t.amount, t.currency, recipient.currency)
            if converted <= 0:
                raise InvalidOperationError("После конвертации сумма меньше минимальной денежной единицы.")
        external_fee = Decimal("0")
        if t.type is TransactionType.EXTERNAL_TRANSFER:
            external_fee = (t.amount * self.external_fee_rate).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)
        premium_fee = sender.fee if isinstance(sender, PremiumAccount) else Decimal("0")
        # withdraw премиального счёта самостоятельно добавляет свою комиссию.
        t._update(fee=external_fee + premium_fee)
        sender_before = sender.balance
        recipient_before = recipient.balance if recipient is not None else None
        try:
            with _access_accounts(self.bank):
                sender.withdraw(t.amount + external_fee)
                if recipient is not None:
                    recipient.deposit(converted)
        except Exception:
            # Атомарность в памяти: при ошибке зачисления возвращаем оба баланса.
            sender._balance = sender_before
            if recipient is not None:
                recipient._balance = recipient_before
            raise
        t._update(recipient_amount=converted if recipient is not None else (
            t.amount if t.type is TransactionType.EXTERNAL_TRANSFER else Decimal("0")))

    def _record_audit(self, level, event, **details):
        before = len(self.audit._records)
        try:
            self.audit.record(level, event, **details)
        except Exception as error:
            # Даже нестандартный журнал не должен отменять уже проведённые деньги.
            if len(self.audit._records) == before:
                self.audit._defer(level, event, **details)
            self.audit._note_write_error(error)

    def execute_now(self, transaction):
        """Синхронный путь Bank использует тот же обработчик и тот же риск."""
        if transaction.scheduled_at > self._clock():
            raise InvalidOperationError("Отложенную операцию нельзя провести немедленно.")
        self.queue.add(transaction)
        return self._process_transaction(transaction)

    def _process_transaction(self, t):
        if t.transaction_id in self.bank._completed_transactions:
            raise InvalidOperationError("Транзакция уже проведена.")
        if t.status not in (TransactionStatus.QUEUED, TransactionStatus.RETRY):
            raise InvalidOperationError("Операция не ожидает исполнения.")
        registered = self.bank._transactions.get(t.transaction_id)
        if registered is not None and registered is not t:
            raise InvalidOperationError("ID уже принадлежит другой транзакции.")
        self.bank._transactions[t.transaction_id] = t
        t._transition(TransactionStatus.PROCESSING, self._clock(), attempts=t.attempts + 1)
        self.execution_order.append(t.transaction_id)
        try:
            if self.before_execute is not None:
                self.before_execute(t)
            self._execute(t)
        except Exception as error:
            record = {"transaction_id": t.transaction_id, "attempt": t.attempts,
                      "time": self._clock().isoformat(), "error_type": type(error).__name__,
                      "message": str(error)}
            t._errors.append(record.copy())
            self.error_log.append(record)
            t._last_error = error
            if isinstance(error, TemporaryTransactionError) and t.attempts < self.max_attempts:
                t._transition(TransactionStatus.RETRY, self._clock(), failure_reason=str(error),
                              scheduled_at=self._clock() + timedelta(seconds=self.retry_delay))
            else:
                t._transition(TransactionStatus.FAILED, self._clock(), failure_reason=str(error))
            self._record_audit(AuditLevel.ERROR, "transaction_error",
                               client_id=self.bank._sessions.get(t._session),
                               transaction_id=t.transaction_id, details=record)
        else:
            # Реестр фиксируется ДО журналирования: сбой файла не разрешит повтор денег.
            completed = self._clock()
            self.bank._completed_transactions[t.transaction_id] = completed
            t._last_error = None
            t._transition(TransactionStatus.COMPLETED, completed,
                          failure_reason=None, completed_at=completed)
            client_id = self.bank._sessions.get(t._session)
            self.risk_analyzer.mark_completed(t, client_id)
            related = []
            if t.type is TransactionType.TRANSFER:
                related.append(self.bank._owners[t.recipient])
            self._record_audit(AuditLevel.INFO, "transaction_completed", client_id=client_id,
                               related_client_ids=related, transaction_id=t.transaction_id,
                               details={"type": t.type.value, "amount": str(t.amount),
                                        "currency": t.currency, "fee": str(t.fee),
                                        "sender": t.sender, "recipient": t.recipient,
                                        "recipient_amount": str(t.recipient_amount),
                                        "attempts": t.attempts})
        return t

    def process_ready(self):
        return [self._process_transaction(t) for t in self.queue.ready()]


def demonstrate_day4():
    print("\nДЕНЬ 4 — 10 ТРАНЗАКЦИЙ")
    time = [datetime(2026, 10, 13, 12, 0)]
    clock = lambda: time[0]
    bank = Bank("Банк транзакций", clock=clock)
    alice = Client("Алиса", 25, {"phone": "111"})
    bob = Client("Боб", 30, {"phone": "222"})
    bank.add_client(alice, "пароль")
    bank.add_client(bob, "пароль")
    sa = bank.authenticate_client(alice.client_id, "пароль")
    sb = bank.authenticate_client(bob.client_id, "пароль")
    source = bank.open_account(sa, balance="10000")
    rub = bank.open_account(sb)
    usd = bank.open_account(sb, currency="USD")
    frozen = bank.open_account(sb)
    bank.freeze_account(sb, frozen)
    premium = bank.open_account(sa, "premium", balance="100", overdraft_limit="500", fee="10")
    queue = TransactionQueue(clock)

    def make(kind, amount, **kwargs):
        return Transaction(kind, amount, "RUB", session=sa, clock=clock, **kwargs)

    transactions = [
        make(TransactionType.TRANSFER, "100", sender=source, recipient=rub, priority=5),
        make(TransactionType.TRANSFER, "1000", sender=source, recipient=usd, priority=10),
        make(TransactionType.EXTERNAL_TRANSFER, "500", sender=source, recipient="Внешний банк: 1234"),
        make(TransactionType.TRANSFER, "100", sender=source, recipient=frozen),
        make(TransactionType.TRANSFER, "99999", sender=source, recipient=rub),
        make(TransactionType.TRANSFER, "200", sender=premium, recipient=rub),
        make(TransactionType.TRANSFER, "50", sender=source, recipient=rub,
             scheduled_at=time[0] + timedelta(minutes=10)),
        make(TransactionType.TRANSFER, "30", sender=source, recipient=rub),
        make(TransactionType.TRANSFER, "40", sender=source, recipient=rub),
        make(TransactionType.DEPOSIT, "300", recipient=source),
    ]
    for transaction in transactions:
        queue.add(transaction)
    queue.cancel(transactions[7].transaction_id)

    def temporary_failure(transaction):
        if transaction is transactions[8] and transaction.attempts == 1:
            raise TemporaryTransactionError("Учебный временный сбой")

    # Здесь показываем очередь и комиссии. Стандартный порог частоты (5)
    # отдельно проверяется в дне 5; для пакета дня 4 используем порог 100.
    audit = AuditLog(clock=clock)
    risk = RiskAnalyzer(audit, frequency_limit=100, clock=clock)
    processor = TransactionProcessor(bank, queue, clock=clock, before_execute=temporary_failure,
                                     audit=audit, risk_analyzer=risk)
    processor.process_ready()
    assert processor.execution_order[:2] == [transactions[1].transaction_id, transactions[0].transaction_id]
    assert transactions[6].status is TransactionStatus.QUEUED
    assert transactions[7].status is TransactionStatus.CANCELLED
    assert transactions[8].status is TransactionStatus.RETRY
    assert transactions[2].fee == Decimal("5.00")
    assert transactions[1].recipient_amount == Decimal("10.00")
    assert bank.search_accounts(sa, account_id=premium)[0]["balance"] == Decimal("-110")
    # Повторный запуск без продвижения времени ничего не списывает.
    assert processor.process_ready() == []
    time[0] += timedelta(minutes=1)
    processor.process_ready()
    assert transactions[8].status is TransactionStatus.COMPLETED
    assert transactions[8].attempts == 2
    time[0] += timedelta(minutes=9)
    processor.process_ready()
    assert transactions[6].status is TransactionStatus.COMPLETED
    assert processor.process_ready() == []
    assert sum(t.status is TransactionStatus.COMPLETED for t in transactions) == 7
    assert sum(t.status is TransactionStatus.FAILED for t in transactions) == 2
    assert bank.search_accounts(sa, account_id=source)[0]["balance"] == Decimal("8605")
    assert bank.search_accounts(sb, account_id=rub)[0]["balance"] == Decimal("390")
    assert bank.search_accounts(sb, account_id=usd)[0]["balance"] == Decimal("10")
    for transaction in transactions:
        print(transaction)
    print("Журнал ошибок:", processor.error_log)
    print("Все проверки четвёртого дня пройдены: 7 выполнены, 2 отклонены, 1 отменена.")



    
# День 5 — Аудит и анализ риска. Пороги — правила учебной модели.


class AuditLevel(Enum):
    INFO = "информация"
    WARNING = "предупреждение"
    ERROR = "ошибка"
    CRITICAL = "критический"


class RiskLevel(Enum):
    LOW = "низкий"
    MEDIUM = "средний"
    HIGH = "высокий"


class RiskBlockedError(InvalidOperationError):
    pass


class AuditLog:
    """JSONL: одна JSON-запись на строку. Пароли и сессии не записываем.

    Событие сначала сохраняется в памяти. Ошибка файла видна в write_errors,
    несохранённые события — в pending_count. flush() повторяет только запись
    журнала, а не денежную операцию. Файл обновляется атомарной заменой.
    Фильтрация относится к записям текущего экземпляра в памяти.
    Одновременная запись из нескольких процессов не поддерживается.
    """
    def __init__(self, file_path=None, clock=None):
        self._clock = clock or (lambda: datetime.now(ZoneInfo("Europe/Moscow")))
        self._records = []
        self._pending = []
        self._write_errors = []
        self._file_path = Path(file_path) if file_path is not None else None
        if self._file_path is not None:
            self._file_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def pending_count(self):
        return len(self._pending)

    @property
    def write_errors(self):
        return deepcopy(self._write_errors)

    def _note_write_error(self, error):
        self._write_errors.append({"timestamp": self._clock().isoformat(),
                                   "error_type": type(error).__name__, "message": str(error)})

    def _defer(self, level, event, *, client_id=None, transaction_id=None, details=None,
               related_client_ids=None):
        if not isinstance(level, AuditLevel):
            raise InvalidOperationError("Укажите AuditLevel.")
        if not isinstance(event, str) or not event.strip():
            raise InvalidOperationError("Название события должно быть непустым.")
        entry = {"event_id": uuid4().hex,
                 "timestamp": self._clock().isoformat(), "level": level.value,
                 "event": event, "client_id": client_id, "transaction_id": transaction_id,
                 "related_client_ids": list(dict.fromkeys(related_client_ids or [])),
                 "details": deepcopy(details) if details is not None else {}}
        json.dumps(entry, ensure_ascii=False, allow_nan=False)
        self._records.append(entry)
        if self._file_path is not None:
            self._pending.append(entry)
        return deepcopy(entry)

    def record(self, level, event, *, client_id=None, transaction_id=None, details=None,
               related_client_ids=None):
        entry = self._defer(level, event, client_id=client_id, transaction_id=transaction_id,
                            details=details, related_client_ids=related_client_ids)
        self.flush()
        return entry

    def flush(self):
        """True — все события записаны; False — остались в памяти для повтора."""
        if self._file_path is None or not self._pending:
            return True
        temporary = None
        try:
            existing = self._file_path.read_text(encoding="utf-8") if self._file_path.exists() else ""
            known = {json.loads(line).get("event_id") for line in existing.splitlines() if line.strip()}
            entries = [entry for entry in self._pending if entry["event_id"] not in known]
            addition = "".join(json.dumps(entry, ensure_ascii=False, allow_nan=False) + "\n"
                               for entry in entries)
            if existing and not existing.endswith("\n"):
                existing += "\n"
            with NamedTemporaryFile(mode="w", encoding="utf-8", dir=self._file_path.parent,
                                    prefix=".audit-", delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(existing + addition)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self._file_path)
            self._pending.clear()
            return True
        except (OSError, ValueError) as error:
            self._note_write_error(error)
            return False
        finally:
            if temporary is not None:
                try:
                    temporary.unlink(missing_ok=True)
                except OSError as error:
                    self._note_write_error(error)

    def filter(self, *, level=None, event=None, client_id=None,
               transaction_id=None, since=None, until=None):
        if level is not None and not isinstance(level, AuditLevel):
            raise InvalidOperationError("Укажите AuditLevel.")
        result = []
        for entry in self._records:
            if level is not None and entry["level"] != level.value:
                continue
            if event is not None and entry["event"] != event:
                continue
            if (client_id is not None and entry["client_id"] != client_id
                    and client_id not in entry.get("related_client_ids", [])):
                continue
            if transaction_id is not None and entry["transaction_id"] != transaction_id:
                continue
            timestamp = datetime.fromisoformat(entry["timestamp"])
            if since is not None and timestamp < since:
                continue
            if until is not None and timestamp > until:
                continue
            result.append(deepcopy(entry))
        return result

    def suspicious_operations(self):
        return [entry for entry in self.filter(event="risk_assessment")
                if entry["details"]["risk"] != RiskLevel.LOW.value]

    def error_statistics(self):
        errors = self.filter(event="transaction_error")
        counts = {}
        for entry in errors:
            kind = entry["details"]["error_type"]
            counts[kind] = counts.get(kind, 0) + 1
        # Каждая неудачная попытка учитывается отдельно.
        return {"failed_attempts": len(errors),
                "affected_transactions": len({entry["transaction_id"] for entry in errors}),
                "by_error_type": counts}


class RiskAnalyzer:
    """Суммы сравниваются в условных RUB после конвертации.

    Крупная сумма >= 100000: 2 балла.
    Частота >= 5 новых транзакций за 60 секунд: 2 балла.
    Новый получатель перевода: 1 балл.
    Ночная попытка (00:00–05:00): 2 балла.
    0 баллов — низкий риск, 1 — средний, >= 2 — высокий и запрет.
    Получатель считается знакомым после успешного перевода этому клиенту.
    Повтор одной транзакции не увеличивает частоту, но риск переоценивается.
    """
    def __init__(self, audit, *, large_amount="100000", frequency_limit=5,
                 window_seconds=60, clock=None):
        if not isinstance(audit, AuditLog):
            raise InvalidOperationError("Передайте AuditLog.")
        for value in (frequency_limit, window_seconds):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise InvalidOperationError("Частота и окно должны быть положительными целыми.")
        self.audit = audit
        self.large_amount = BankAccount._validate_amount(large_amount)
        self.frequency_limit = frequency_limit
        self.window_seconds = window_seconds
        self._clock = clock or audit._clock
        self._attempts = {}
        self._known_recipients = {}
        self._assessments = {}

    def assess(self, transaction, client_id, amount_rub):
        now = self._clock()
        attempts = self._attempts.setdefault(client_id, {})
        cutoff = now - timedelta(seconds=self.window_seconds)
        attempts = {number: stamp for number, stamp in attempts.items() if stamp > cutoff}
        attempts.setdefault(transaction.transaction_id, now)
        self._attempts[client_id] = attempts
        reasons = []
        score = 0
        if amount_rub >= self.large_amount:
            score += 2
            reasons.append("Крупная сумма")
        if len(attempts) >= self.frequency_limit:
            score += 2
            reasons.append("Частые операции")
        if transaction.type in (TransactionType.TRANSFER, TransactionType.EXTERNAL_TRANSFER):
            recipient_key = (transaction.type.value, transaction.recipient)
            if recipient_key not in self._known_recipients.get(client_id, set()):
                score += 1
                reasons.append("Новый получатель")
        if 0 <= now.hour < 5:
            score += 2
            reasons.append("Ночная операция")
        level = RiskLevel.HIGH if score >= 2 else RiskLevel.MEDIUM if score else RiskLevel.LOW
        assessment = {"risk": level.value, "score": score, "reasons": reasons,
                      "amount_rub": str(amount_rub), "operations_in_window": len(attempts)}
        self.audit.record(AuditLevel.CRITICAL if level is RiskLevel.HIGH else
                          AuditLevel.WARNING if level is RiskLevel.MEDIUM else AuditLevel.INFO,
                          "risk_assessment", client_id=client_id,
                          transaction_id=transaction.transaction_id, details=assessment)
        self._assessments[transaction.transaction_id] = {
            "client_id": client_id, **assessment, "timestamp": now.isoformat()}
        return level, assessment

    def mark_completed(self, transaction, client_id):
        if transaction.type in (TransactionType.TRANSFER, TransactionType.EXTERNAL_TRANSFER):
            self._known_recipients.setdefault(client_id, set()).add(
                (transaction.type.value, transaction.recipient))

    def client_risk_profile(self, client_id):
        # Последняя оценка каждой уникальной операции, без двойного счёта повторов.
        rows = [row for row in self._assessments.values() if row["client_id"] == client_id]
        counts = {level.value: sum(row["risk"] == level.value for row in rows) for level in RiskLevel}
        overall = RiskLevel.HIGH if counts[RiskLevel.HIGH.value] else (
            RiskLevel.MEDIUM if counts[RiskLevel.MEDIUM.value] else RiskLevel.LOW)
        return {"client_id": client_id, "risk": overall.value, "transaction_count": len(rows),
                "by_risk_level": counts,
                "reasons": sorted({reason for row in rows for reason in row["reasons"]})}


def demonstrate_day5():
    print("\nДЕНЬ 5 — АУДИТ И РИСК-АНАЛИЗ")
    time = [datetime(2026, 10, 13, 12)]
    clock = lambda: time[0]
    bank = Bank("Банк аудита", clock)
    alice = Client("Клиент аудита", 25, {"phone": "333"})
    bob = Client("Получатель", 30, {"phone": "444"})
    bank.add_client(alice, "пароль")
    bank.add_client(bob, "пароль")
    session = bank.authenticate_client(alice.client_id, "пароль")
    other = bank.authenticate_client(bob.client_id, "пароль")
    source = bank.open_account(session, balance="500000")
    target = bank.open_account(other)
    queue = TransactionQueue(clock)
    # Временный файл демонстрации удаляется после проверки.
    with TemporaryDirectory(prefix="bank-audit-") as directory:
        path = Path(directory) / "audit.jsonl"
        audit = AuditLog(path, clock)
        risk = RiskAnalyzer(audit, clock=clock)
        processor = TransactionProcessor(bank, queue, clock=clock, audit=audit, risk_analyzer=risk)

        def transfer(amount):
            transaction = Transaction(TransactionType.TRANSFER, amount, "RUB", sender=source,
                                      recipient=target, session=session, clock=clock)
            queue.add(transaction)
            processor.process_ready()
            return transaction

        first = transfer("100")  # Новый получатель: средний риск, разрешено.
        assert first.status is TransactionStatus.COMPLETED
        second = transfer("100")  # Уже знакомый получатель: низкий риск.
        assert risk._assessments[second.transaction_id]["risk"] == RiskLevel.LOW.value
        big = transfer("100000")
        assert big.status is TransactionStatus.FAILED and 'Высокий риск' in big.failure_reason
        fourth = transfer("100")
        frequent = transfer("100")  # Пятая попытка в том же окне.
        assert fourth.status is TransactionStatus.COMPLETED
        assert frequent.status is TransactionStatus.FAILED
        assert "Частые операции" in risk._assessments[frequent.transaction_id]["reasons"]
        time[0] = datetime(2026, 10, 14, 2)
        night = transfer("100")
        assert night.status is TransactionStatus.FAILED
        assert "Ночная операция" in risk._assessments[night.transaction_id]["reasons"]
        assert bank.search_accounts(session, account_id=source)[0]["balance"] == Decimal("499700")
        assert bank.search_accounts(other, account_id=target)[0]["balance"] == Decimal("300")
        assert len(audit.filter(event="transaction_error")) == 3
        assert len(audit.filter(level=AuditLevel.CRITICAL)) == 3
        assert audit.error_statistics()["by_error_type"]["RiskBlockedError"] == 3
        assert risk.client_risk_profile(alice.client_id)["transaction_count"] == 6
        entries = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        assert entries == audit.filter()
        print("Подозрительные операции:", audit.suspicious_operations())
        print("Риск-профиль:", risk.client_risk_profile(alice.client_id))
        print("Статистика ошибок:", audit.error_statistics())
        print(f"Запись в JSONL проверена: {len(entries)} событий.")
    print("Все проверки пятого дня пройдены.")


# День 6 — Комплексная демонстрация всей системы.


def demonstrate_day6(output_dir=None, *, return_context=False):
    """6 клиентов, 12 счетов и 40 транзакций. Часы учебные, ожидания нет.

    Журнал и отчёт перезаписываются при новом запуске этой демонстрации.
    Общая валюта RUB позволяет получить единый топ-3 без смешивания валют.
    """
    print("\nДЕНЬ 6 — КОМПЛЕКСНАЯ ДЕМОНСТРАЦИЯ")
    output_dir = Path(output_dir) if output_dir is not None else Path(__file__).resolve().parent
    output_dir.mkdir(parents=True, exist_ok=True)
    audit_path = output_dir / "audit_day6.jsonl"
    audit_path.write_text("", encoding="utf-8")
    current = [datetime(2026, 10, 15, 12, 0, tzinfo=ZoneInfo("Europe/Moscow"))]
    clock = lambda: current[0]
    bank = Bank("Демонстрационный банк", clock)
    audit = AuditLog(audit_path, clock)
    risk = RiskAnalyzer(audit, clock=clock)
    queue = TransactionQueue(clock)
    clients, sessions, accounts = [], [], []
    names = ["Анна Иванова", "Борис Петров", "Вера Сидорова",
             "Глеб Орлов", "Дарья Смирнова", "Егор Волков"]
    for index, name in enumerate(names):
        client = Client(name, 20 + index, {"email": f"client{index}@example.test"})
        bank.add_client(client, f"учебный-пароль-{index}")
        session = bank.authenticate_client(client.client_id, f"учебный-пароль-{index}")
        main = bank.open_account(session, "bank", balance=str(20000 + index * 1000))
        kind = ("savings", "premium", "investment")[index % 3]
        settings = {"min_balance": "500"} if kind == "savings" else (
            {"overdraft_limit": "5000", "fee": "10"} if kind == "premium" else {})
        extra = bank.open_account(session, kind, balance="10000", **settings)
        if kind == "investment":
            bank.invest(session, extra, "etf", "1000")
        clients.append(client)
        sessions.append(session)
        accounts.append((main, extra))
    assert len(bank._clients) == 6 and len(bank._accounts) == 12
    bank.freeze_account(sessions[5], accounts[5][1])
    initial_total = bank.get_total_balance()["RUB"]
    # Аудит симуляции начинается после формирования начальных портфелей.
    processor = TransactionProcessor(bank, queue, audit=audit, risk_analyzer=risk, clock=clock)
    balance_history = [{"timestamp": clock().isoformat(), "totals": bank.get_total_balance()}]
    transactions = []

    def add_transaction(client_index, kind, amount, *, sender=None, recipient=None,
                        priority=0, scheduled_at=None):
        transaction = Transaction(kind, amount, "RUB", sender=sender, recipient=recipient,
                                  session=sessions[client_index], priority=priority,
                                  scheduled_at=scheduled_at, clock=clock)
        queue.add(transaction)
        transactions.append(transaction)
        audit.record(AuditLevel.INFO, "transaction_queued",
                     client_id=clients[client_index].client_id,
                     transaction_id=transaction.transaction_id,
                     details={"type": kind.value, "amount": str(transaction.amount),
                              "currency": transaction.currency, "priority": priority,
                              "scheduled_at": transaction.scheduled_at.isoformat()})
        print(f"В ОЧЕРЕДИ: {transaction.transaction_id[:8]} | {kind.value} | {amount} RUB")
        return transaction

    def process_and_print():
        processed = processor.process_ready()
        if processed:
            balance_history.append({"timestamp": clock().isoformat(), "totals": bank.get_total_balance()})
        for transaction in processed:
            label = "ИСПОЛНЕНО" if transaction.status is TransactionStatus.COMPLETED else "ОТКЛОНЕНО"
            print(f"{label}: {transaction.transaction_id[:8]} | "
                  f"{transaction.failure_reason or 'операция завершена'}")

    # Пять быстрых переводов одного клиента: пятый блокируется за частоту.
    for index in range(5):
        add_transaction(0, TransactionType.TRANSFER, "100",
                        sender=accounts[0][0], recipient=accounts[1][0], priority=index)
    process_and_print()
    assert sum(t.status is TransactionStatus.FAILED for t in transactions) == 1

    # Ещё 35 операций. Раздвигаем время, чтобы показать отдельные причины отказа.
    delayed = None
    for index in range(35):
        current[0] += timedelta(minutes=2)
        client_index = index % 6
        main, extra = accounts[client_index]
        target = accounts[(client_index + 1) % 6][0]
        scenario = index % 10
        if scenario == 0:
            add_transaction(client_index, TransactionType.TRANSFER, "100000",
                            sender=main, recipient=target)  # Высокий риск: крупная сумма.
        elif scenario == 1:
            add_transaction(client_index, TransactionType.TRANSFER, "50",
                            sender=main, recipient=accounts[5][1])  # Замороженный получатель.
        elif scenario == 2:
            add_transaction(client_index, TransactionType.WITHDRAWAL, "90000", sender=main)
        elif scenario == 3:
            add_transaction(client_index, TransactionType.EXTERNAL_TRANSFER, "200",
                            sender=main, recipient="Учебный внешний банк: 1234")
        elif scenario == 4:
            add_transaction(client_index, TransactionType.DEPOSIT, "300", recipient=main)
        elif scenario == 5:
            add_transaction(client_index, TransactionType.TRANSFER, "100",
                            sender=main, recipient=target,
                            scheduled_at=current[0] + timedelta(minutes=3))
        elif scenario == 6:
            add_transaction(client_index, TransactionType.WITHDRAWAL, "50", sender=extra)
        elif scenario == 7:
            add_transaction(client_index, TransactionType.TRANSFER, "50",
                            sender=target, recipient=main)  # Чужой счёт отправителя.
        elif scenario == 8:
            add_transaction(client_index, TransactionType.TRANSFER, "150", sender=main, recipient=target)
        else:
            if delayed is None:
                # Одна операция назначена на ночное время.
                night = current[0].replace(hour=2, minute=0) + timedelta(days=1)
                delayed = add_transaction(client_index, TransactionType.TRANSFER, "100",
                                          sender=main, recipient=target, scheduled_at=night)
            else:
                add_transaction(client_index, TransactionType.TRANSFER, "75", sender=main, recipient=target)
        process_and_print()
    current[0] += timedelta(minutes=10)
    process_and_print()
    assert delayed is not None and delayed.status is TransactionStatus.QUEUED
    current[0] = delayed.scheduled_at
    process_and_print()
    assert delayed.status is TransactionStatus.FAILED and "Ночная операция" in delayed.failure_reason
    assert len(transactions) == 40
    assert all(t.status in (TransactionStatus.COMPLETED, TransactionStatus.FAILED) for t in transactions)
    assert processor.process_ready() == []

    # Проверяем движение средств: внутренние переводы не меняют общий баланс.
    expected_total = initial_total
    for transaction in transactions:
        if transaction.status is not TransactionStatus.COMPLETED:
            continue
        if transaction.type is TransactionType.DEPOSIT:
            expected_total += transaction.amount
        elif transaction.type in (TransactionType.WITHDRAWAL, TransactionType.EXTERNAL_TRANSFER):
            expected_total -= transaction.amount + transaction.fee
        else:
            expected_total -= transaction.fee
    assert bank.get_total_balance()["RUB"] == expected_total

    print("\nСЧЕТА КЛИЕНТА:", clients[0].full_name)
    client_accounts = bank.search_accounts(sessions[0])
    for info in client_accounts:
        print(info)
    print("\nИСТОРИЯ КЛИЕНТА:")
    history = audit.filter(client_id=clients[0].client_id)
    for entry in history:
        print(entry["timestamp"], entry["event"], entry["transaction_id"], entry["details"])
    suspicious = audit.suspicious_operations()
    print("\nПОДОЗРИТЕЛЬНЫЕ ОПЕРАЦИИ:")
    for entry in suspicious:
        print(entry["transaction_id"], entry["details"]["risk"], entry["details"]["reasons"])
    counts = {status.value: sum(t.status is status for t in transactions)
              for status in TransactionStatus}
    ranking = bank.get_clients_ranking()["RUB"][:3]
    total = bank.get_total_balance()
    report = {"clients": len(clients), "accounts": len(bank._accounts),
              "transactions": len(transactions), "transaction_statistics": counts,
              "suspicious_assessments": len(suspicious),
              "blocked_by_risk": sum(t.status is TransactionStatus.FAILED
                                     and t.failure_reason.startswith("Высокий риск") for t in transactions),
              "error_statistics": audit.error_statistics(), "top_3_RUB": ranking,
              "total_balance_by_currency": total, "client_accounts": client_accounts,
              "client_history": history,
              "risk_profiles": [risk.client_risk_profile(c.client_id) for c in clients]}
    report_path = output_dir / "report_day6.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("\nТОП-3 КЛИЕНТОВ (RUB):", ranking)
    print("СТАТИСТИКА ТРАНЗАКЦИЙ:", counts)
    print("ОБЩИЙ БАЛАНС ПО ВАЛЮТАМ:", total)
    assert len(audit.filter(event="transaction_queued")) == 40
    assert len(audit.filter(event="transaction_completed")) == counts[TransactionStatus.COMPLETED.value]
    assert len(audit.filter(event="transaction_error")) == counts[TransactionStatus.FAILED.value]
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert entries == audit.filter()
    assert json.loads(report_path.read_text(encoding="utf-8"))["transactions"] == 40
    print("Все проверки шестого дня пройдены.")
    print(f"Аудит: {audit_path}\nОтчёт: {report_path}")
    if return_context:
        return {"bank": bank, "audit": audit, "risk": risk, "transactions": transactions,
                "balance_history": balance_history, "clients": clients, "report": report}
    return report


# День 7 — Отчёты и визуализация. Matplotlib нужен только для save_charts().


class ReportBuilder:
    """Отчёты используют текущий банк и реальные снимки его состояния.

    Денежные Decimal экспортируются строками без потери точности.
    CSV имеет колонки path/value: вложенные данные разворачиваются в строки.
    Отчёты управленческие; авторизацию интерфейса пользователя выполняет Bank.
    """
    def __init__(self, bank, audit, risk_analyzer, transactions, balance_history=None):
        self.bank = bank
        self.audit = audit
        self.risk_analyzer = risk_analyzer
        self.transactions = list(transactions)
        self.balance_history = deepcopy(balance_history or [])

    @staticmethod
    def _transaction_info(transaction):
        return {"id": transaction.transaction_id, "type": transaction.type.value,
                "sender": transaction.sender, "recipient": transaction.recipient,
                "amount": transaction.amount, "currency": transaction.currency,
                "fee": transaction.fee, "status": transaction.status.value,
                "failure_reason": transaction.failure_reason,
                "created_at": transaction.created_at.isoformat(),
                "completed_at": transaction.completed_at.isoformat() if transaction.completed_at else None}

    @money_calculation
    def build_client_report(self, client_id):
        client = self.bank._get_client(client_id)
        numbers = set(client.account_ids)
        accounts = [self.bank._accounts[number].get_account_info() for number in client.account_ids]
        totals = {currency: Decimal("0") for currency in sorted(BankAccount.ALLOWED_CURRENCIES)}
        for info in accounts:
            if info["status"] != AccountStatus.CLOSED.value:
                totals[info["currency"]] += info.get("total_assets", info["balance"])
        return {"report_type": "client", "client_id": client_id, "full_name": client.full_name,
                "status": client.status.value, "accounts": accounts,
                "total_balance_by_currency": totals,
                "transactions": [self._transaction_info(t) for t in self.transactions
                                 if t.sender in numbers or t.recipient in numbers],
                "risk_profile": self.risk_analyzer.client_risk_profile(client_id)}

    def build_bank_report(self):
        counts = {status.value: sum(t.status is status for t in self.transactions)
                  for status in TransactionStatus}
        return {"report_type": "bank", "name": self.bank.name,
                "client_count": len(self.bank._clients), "account_count": len(self.bank._accounts),
                "transaction_count": len(self.transactions), "transaction_statistics": counts,
                "total_balance_by_currency": self.bank.get_total_balance(),
                "top_3_by_currency": {currency: rows[:3]
                                      for currency, rows in self.bank.get_clients_ranking().items()},
                "balance_history": deepcopy(self.balance_history)}

    def build_risk_report(self):
        profiles = [self.risk_analyzer.client_risk_profile(client.client_id)
                    for client in self.bank._clients.values()]
        levels = {level.value: sum(row["risk"] == level.value
                                  for row in self.risk_analyzer._assessments.values())
                  for level in RiskLevel}
        return {"report_type": "risks", "assessment_counts": levels,
                "unassessed_transaction_count": sum(
                    t.transaction_id not in self.risk_analyzer._assessments for t in self.transactions),
                "suspicious_operations": self.audit.suspicious_operations(),
                "client_risk_profiles": profiles, "error_statistics": self.audit.error_statistics()}

    @staticmethod
    def _flatten(value, path=""):
        if isinstance(value, dict):
            if not value:
                yield path, "{}"
            for key, item in value.items():
                yield from ReportBuilder._flatten(item, f"{path}.{key}" if path else str(key))
        elif isinstance(value, list):
            if not value:
                yield path, "[]"
            for index, item in enumerate(value):
                yield from ReportBuilder._flatten(item, f"{path}[{index}]")
        else:
            yield path, "null" if value is None else str(value)

    @staticmethod
    def _output_path(path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def export_to_text(self, report, path):
        path = self._output_path(path)
        lines = [f"ОТЧЁТ: {report.get('report_type', 'данные')}"]
        lines.extend(f"{key}: {value}" for key, value in self._flatten(report))
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def export_to_json(self, report, path):
        path = self._output_path(path)
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str,
                                   allow_nan=False), encoding="utf-8")
        return path

    def export_to_csv(self, report, path):
        path = self._output_path(path)
        # UTF-8 BOM помогает Excel правильно открыть русский текст.
        with path.open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["path", "value"])
            for key, value in self._flatten(report):
                # Не даём текстовым значениям стать формулами при открытии в Excel.
                if value.startswith(("=", "+", "@")):
                    value = "'" + value
                writer.writerow([key, value])
        return path

    def save_charts(self, output_dir, currency="RUB"):
        if currency not in BankAccount.ALLOWED_CURRENCIES:
            raise InvalidOperationError("Неизвестная валюта графика.")
        # Отдельный writable cache: не пишем в домашний каталог пользователя.
        import os
        import tempfile
        os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "bank-matplotlib-cache"))
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
        except ImportError:
            raise InvalidOperationError("Для графиков установите matplotlib: python3 -m pip install matplotlib") from None
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
        paths = []
        counts = self.build_risk_report()["assessment_counts"]
        fig, ax = plt.subplots(figsize=(8, 5), layout="constrained")
        if sum(counts.values()):
            labels, values = zip(*[(key, value) for key, value in counts.items() if value])
            colors = {"низкий": "#3b9c79", "средний": "#e5b347", "высокий": "#cc5b5b"}
            ax.pie(values, labels=labels, autopct="%1.0f%%", colors=[colors[key] for key in labels])
        else:
            ax.text(0.5, 0.5, "Нет оценённых операций", ha="center", va="center")
            ax.axis("off")
        ax.set_title("Распределение оценённых операций по уровню риска")
        path = output_dir / "risk_pie.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

        rows = self.bank.get_clients_ranking()[currency]
        fig, ax = plt.subplots(figsize=(10, 6), layout="constrained")
        if rows:
            positions = list(range(len(rows)))
            ax.barh(positions, [float(row["total_balance"]) for row in rows], color="#4776a8")
            ax.set_yticks(positions)
            ax.set_yticklabels([f'{row["full_name"]} ({row["client_id"][:6]})' for row in rows])
            ax.invert_yaxis()
        else:
            ax.text(0.5, 0.5, "Нет счетов в выбранной валюте", transform=ax.transAxes, ha="center")
        ax.set_title("Средства клиентов, включая инвестиционный портфель")
        ax.set_xlabel(f"Сумма ({currency})")
        ax.grid(axis="x", alpha=0.2)
        ax.set_axisbelow(True)
        path = output_dir / f"client_balances_{currency}.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)

        fig, ax = plt.subplots(figsize=(10, 5), layout="constrained")
        points = self.balance_history
        if points:
            times = [datetime.fromisoformat(point["timestamp"]) for point in points]
            values = [float(point["totals"][currency]) for point in points]
            # Ступени показывают дискретные изменения, а не плавную доходность.
            ax.step(times, values, where="post", color="#4776a8", linewidth=2)
            tz = times[0].tzinfo
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%d.%m %H:%M", tz=tz))
            ax.tick_params(axis="x", labelrotation=25)
            ax.ticklabel_format(axis="y", style="plain", useOffset=False)
        else:
            ax.text(0.5, 0.5, "История баланса не записана", transform=ax.transAxes, ha="center")
        ax.set_title("Движение общих средств банка по снимкам состояния")
        ax.set_xlabel("Время (Europe/Moscow в демонстрации)")
        ax.set_ylabel(f"Средства ({currency})")
        ax.grid(alpha=0.2)
        path = output_dir / f"balance_history_{currency}.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        paths.append(path)
        return paths


def demonstrate_day7(context):
    print("\nДЕНЬ 7 — ОТЧЁТЫ И ГРАФИКИ")
    builder = ReportBuilder(context["bank"], context["audit"], context["risk"],
                            context["transactions"], context["balance_history"])
    output = Path(__file__).resolve().parent / "day7_reports"
    reports = {"client": builder.build_client_report(context["clients"][0].client_id),
               "bank": builder.build_bank_report(), "risks": builder.build_risk_report()}
    for name, report in reports.items():
        text_path = builder.export_to_text(report, output / f"{name}.txt")
        json_path = builder.export_to_json(report, output / f"{name}.json")
        csv_path = builder.export_to_csv(report, output / f"{name}.csv")
        expected = json.loads(json.dumps(report, ensure_ascii=False, default=str))
        assert json.loads(json_path.read_text(encoding="utf-8")) == expected
        with csv_path.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.reader(stream))
        assert rows[0] == ["path", "value"]
        assert len(rows) == len(list(builder._flatten(report))) + 1
        assert text_path.read_text(encoding="utf-8").startswith("ОТЧЁТ:")
    assert reports["bank"]["transaction_count"] == 40
    assert (sum(reports["risks"]["assessment_counts"].values())
            + reports["risks"]["unassessed_transaction_count"]) == 40
    assert builder.balance_history[-1]["totals"] == context["bank"].get_total_balance()
    charts = builder.save_charts(output)
    for path in charts:
        assert path.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
        print("График:", path)
    print("Отчёты:", output)
    print("Все проверки седьмого дня пройдены.")


if __name__ == "__main__":
    demonstrate()
    demonstrate_day2()
    demonstrate_day3()
    demonstrate_day4()
    demonstrate_day5()
    day6_context = demonstrate_day6(return_context=True)
    demonstrate_day7(day6_context)
