"""

Mã sinh viên:202418930

Họ tên: Dương Tùng Lâm

"""

from __future__ import annotations

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, IntEnum
from typing import Callable, Generic, Iterable, TypeVar

T = TypeVar("T")

_log = logging.getLogger(__name__)


class DeviceStatus(Enum):
    AVAILABLE = "Sẵn sàng"
    BORROWED = "Đang mượn"
    UNDER_MAINTENANCE = "Đang bảo trì"


class AlertSeverity(Enum):
    INFORMATION = "Thông tin"
    WARNING = "Cảnh báo"
    CRITICAL = "Nghiêm trọng"


@dataclass(frozen=True)
class DeviceStatusChangedEventArgs:
    device_id: str
    old_status: DeviceStatus
    new_status: DeviceStatus
    changed_at: datetime


@dataclass(frozen=True)
class DeviceAlertEventArgs:
    device_id: str
    message: str
    severity: AlertSeverity
    occurred_at: datetime


StatusHandler = Callable[[object, DeviceStatusChangedEventArgs], None]
AlertHandler = Callable[[object, DeviceAlertEventArgs], None]
AlertFilter = Callable[[DeviceAlertEventArgs], bool]
Clock = Callable[[], datetime]


class InvalidDeviceReadingException(Exception):
    def __init__(self, device_id: str, invalid_value: int):
        super().__init__(f"Giá trị đo không hợp lệ {invalid_value} từ thiết bị {device_id}")
        self.device_id = device_id
        self.invalid_value = invalid_value


class Subscription:
    def __init__(self, release: Callable[[], None]) -> None:
        self._release = release
        self._active = True
        self._lock = threading.Lock()

    @property
    def active(self) -> bool:
        return self._active

    def dispose(self) -> None:
        with self._lock:
            if not self._active:
                return
            self._active = False
        self._release()

    def __enter__(self) -> Subscription:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.dispose()


class Event(Generic[T]):
    """Mặt công khai của event: chỉ cho đăng ký, không cho phát hay xóa."""

    def __init__(self, source: EventSource[T]) -> None:
        self._source = source

    @property
    def handler_count(self) -> int:
        return self._source.handler_count

    def subscribe(self, handler: Callable[[object, T], None]) -> Subscription:
        return self._source.add(handler)


class EventSource(Generic[T]):
    """Mặt riêng của publisher: nắm quyền phát và xóa."""

    def __init__(self) -> None:
        self._entries: dict[int, Callable[[object, T], None]] = {}
        self._next_id = 0
        self._lock = threading.RLock()
        self.event: Event[T] = Event(self)

    @property
    def handler_count(self) -> int:
        return len(self._snapshot())

    def add(self, handler: Callable[[object, T], None]) -> Subscription:
        if not callable(handler):
            raise TypeError("handler phải là callable")
        with self._lock:
            entry_id = self._next_id
            self._next_id += 1
            self._entries[entry_id] = handler
        return Subscription(lambda: self._remove(entry_id))

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def invoke(self, sender: object, args: T) -> None:
        for handler in self._snapshot():
            try:
                handler(sender, args)
            except Exception:
                _log.exception("Handler %r lỗi khi xử lý sự kiện", handler)

    def _remove(self, entry_id: int) -> None:
        with self._lock:
            self._entries.pop(entry_id, None)

    def _snapshot(self) -> list[Callable[[object, T], None]]:
        # Bản sao đã khử trùng: handler hủy đăng ký giữa chừng không làm lệch vòng lặp,
        # và cùng một handler chỉ được gọi một lần dù có nhiều đăng ký.
        with self._lock:
            return list(dict.fromkeys(self._entries.values()))


class Zone(IntEnum):
    DEAD = 0
    LOW = 1
    NORMAL = 2


def _zone_of(value: int, low_threshold: int) -> Zone:
    if value == 0:
        return Zone.DEAD
    return Zone.LOW if value < low_threshold else Zone.NORMAL


def _validate_percent(device_id: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"Giá trị đo của {device_id} phải là số nguyên, nhận {type(value).__name__}")
    if not 0 <= value <= 100:
        raise InvalidDeviceReadingException(device_id, value)


class TeachingDevice(ABC):
    def __init__(self, device_id: str, name: str, *, clock: Clock = datetime.now) -> None:
        if not device_id:
            raise ValueError("device_id không được rỗng")
        self._id = device_id
        self._name = name
        self._clock = clock
        self._status = DeviceStatus.AVAILABLE
        self._status_changed: EventSource[DeviceStatusChangedEventArgs] = EventSource()
        self._alert_raised: EventSource[DeviceAlertEventArgs] = EventSource()

    @property
    def id(self) -> str:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def status(self) -> DeviceStatus:
        return self._status

    @property
    def status_changed(self) -> Event[DeviceStatusChangedEventArgs]:
        return self._status_changed.event

    @property
    def alert_raised(self) -> Event[DeviceAlertEventArgs]:
        return self._alert_raised.event

    @property
    @abstractmethod
    def kind(self) -> str: ...

    def change_status(self, new_status: DeviceStatus) -> None:
        if not isinstance(new_status, DeviceStatus):
            raise TypeError("new_status phải là DeviceStatus")
        if new_status == self._status:
            return
        old_status = self._status
        self._status = new_status
        self._on_status_changed(
            DeviceStatusChangedEventArgs(self._id, old_status, new_status, self._clock())
        )

    def _raise_alert(self, message: str, severity: AlertSeverity) -> None:
        self._on_alert_raised(DeviceAlertEventArgs(self._id, message, severity, self._clock()))

    def _alert_if_worse(self, old: Zone, new: Zone, low_message: str, dead_message: str) -> None:
        # Chỉ cảnh báo khi vùng xấu đi; ở lại cùng vùng hoặc cải thiện thì im lặng.
        if new >= old:
            return
        if new is Zone.DEAD:
            self._raise_alert(dead_message, AlertSeverity.CRITICAL)
        else:
            self._raise_alert(low_message, AlertSeverity.WARNING)

    def _on_status_changed(self, e: DeviceStatusChangedEventArgs) -> None:
        self._status_changed.invoke(self, e)

    def _on_alert_raised(self, e: DeviceAlertEventArgs) -> None:
        self._alert_raised.invoke(self, e)

    def __repr__(self) -> str:
        return f"{self.kind}({self._id!r}, {self._status.value})"


class ProjectorRemote(TeachingDevice):
    LOW_BATTERY = 20

    def __init__(
        self, device_id: str, name: str, battery_level: int = 100, *, clock: Clock = datetime.now
    ) -> None:
        super().__init__(device_id, name, clock=clock)
        _validate_percent(device_id, battery_level)
        self._battery_level = battery_level

    @property
    def kind(self) -> str:
        return "Projector"

    @property
    def battery_level(self) -> int:
        return self._battery_level

    def update_battery_level(self, new_level: int) -> None:
        _validate_percent(self.id, new_level)
        old_level, self._battery_level = self._battery_level, new_level
        self._alert_if_worse(
            _zone_of(old_level, self.LOW_BATTERY),
            _zone_of(new_level, self.LOW_BATTERY),
            f"Pin yếu: {new_level}%",
            "Pin đã cạn",
        )


class AirConditionerRemote(TeachingDevice):
    def __init__(
        self, device_id: str, name: str, assigned_room: str, *, clock: Clock = datetime.now
    ) -> None:
        super().__init__(device_id, name, clock=clock)
        self._assigned_room = assigned_room
        self._is_responsive = True

    @property
    def kind(self) -> str:
        return "AirConditioner"

    @property
    def assigned_room(self) -> str:
        return self._assigned_room

    @property
    def is_responsive(self) -> bool:
        return self._is_responsive

    def update_response_state(self, is_responsive: bool) -> None:
        if not isinstance(is_responsive, bool):
            raise TypeError("is_responsive phải là bool")
        was_responsive, self._is_responsive = self._is_responsive, is_responsive
        if was_responsive and not is_responsive:
            self._raise_alert(
                f"Điều khiển phòng {self._assigned_room} không phản hồi",
                AlertSeverity.CRITICAL,
            )


class WirelessMicrophone(TeachingDevice):
    LOW_BATTERY = 30
    LOW_SIGNAL = 40

    def __init__(
        self,
        device_id: str,
        name: str,
        battery_level: int = 100,
        signal_quality: int = 100,
        *,
        clock: Clock = datetime.now,
    ) -> None:
        super().__init__(device_id, name, clock=clock)
        _validate_percent(device_id, battery_level)
        _validate_percent(device_id, signal_quality)
        self._battery_level = battery_level
        self._signal_quality = signal_quality

    @property
    def kind(self) -> str:
        return "Microphone"

    @property
    def battery_level(self) -> int:
        return self._battery_level

    @property
    def signal_quality(self) -> int:
        return self._signal_quality

    def update_battery_level(self, new_level: int) -> None:
        _validate_percent(self.id, new_level)
        old_level, self._battery_level = self._battery_level, new_level
        self._alert_if_worse(
            _zone_of(old_level, self.LOW_BATTERY),
            _zone_of(new_level, self.LOW_BATTERY),
            f"Pin yếu: {new_level}%",
            "Pin đã cạn",
        )

    def update_signal_quality(self, new_quality: int) -> None:
        _validate_percent(self.id, new_quality)
        old_quality, self._signal_quality = self._signal_quality, new_quality
        self._alert_if_worse(
            _zone_of(old_quality, self.LOW_SIGNAL),
            _zone_of(new_quality, self.LOW_SIGNAL),
            f"Tín hiệu yếu: {new_quality}%",
            "Mất tín hiệu",
        )


class ConsoleNotificationService:
    def handle_alert(self, sender: object, e: DeviceAlertEventArgs) -> None:
        print(f"[{e.severity.value}] {e.device_id}: {e.message}")


class DeviceAuditLogger:
    def __init__(self) -> None:
        self._entries: list[str] = []

    @property
    def entries(self) -> tuple[str, ...]:
        return tuple(self._entries)

    def handle_status_changed(self, sender: object, e: DeviceStatusChangedEventArgs) -> None:
        self._entries.append(
            f"{e.changed_at:%Y-%m-%d %H:%M:%S}: thiết bị {e.device_id}, "
            f"{e.old_status.value} -> {e.new_status.value}"
        )


class MaintenanceService:
    def __init__(self) -> None:
        self._requests: list[str] = []

    @property
    def requests(self) -> tuple[str, ...]:
        return tuple(self._requests)

    def handle_critical_alert(self, sender: object, e: DeviceAlertEventArgs) -> None:
        if e.severity != AlertSeverity.CRITICAL:
            return
        self._requests.append(f"Yêu cầu bảo trì cho {e.device_id}: {e.message}")


class FilteredAlertSubscriber:
    def __init__(self, alert_filter: AlertFilter, action: Callable[[DeviceAlertEventArgs], None]):
        self._filter = alert_filter
        self._action = action

    def handle_alert(self, sender: object, e: DeviceAlertEventArgs) -> None:
        if self._filter(e):
            self._action(e)


class DeviceMonitoringCoordinator:
    def __init__(
        self,
        devices: Iterable[TeachingDevice],
        notification_service: ConsoleNotificationService,
        audit_logger: DeviceAuditLogger,
        maintenance_service: MaintenanceService,
        extra_alert_handlers: Iterable[AlertHandler] = (),
        extra_status_handlers: Iterable[StatusHandler] = (),
    ) -> None:
        self._devices: dict[str, TeachingDevice] = {}
        self._status_handlers: list[StatusHandler] = [
            audit_logger.handle_status_changed,
            *extra_status_handlers,
        ]
        self._alert_handlers: list[AlertHandler] = [
            notification_service.handle_alert,
            maintenance_service.handle_critical_alert,
            *extra_alert_handlers,
        ]
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._subscribed = False
        self._disposed = False

        for device in devices:
            self.add_device(device)

    @property
    def devices(self) -> tuple[TeachingDevice, ...]:
        return tuple(self._devices.values())

    @property
    def is_subscribed(self) -> bool:
        return self._subscribed

    def add_device(self, device: TeachingDevice) -> None:
        self._ensure_alive()
        if device.id in self._devices:
            raise ValueError(f"Thiết bị đã tồn tại: {device.id}")
        if self._subscribed:
            self._subscriptions[device.id] = self._bind(device)
        self._devices[device.id] = device

    def remove_device(self, device_id: str) -> None:
        self._ensure_alive()
        if device_id not in self._devices:
            raise KeyError(f"Không tìm thấy thiết bị: {device_id}")
        del self._devices[device_id]
        self._release(self._subscriptions.pop(device_id, []))

    def add_alert_handler(self, handler: AlertHandler) -> None:
        self._ensure_alive()
        if not callable(handler):
            raise TypeError("handler phải là callable")
        if handler in self._alert_handlers:
            return
        self._alert_handlers.append(handler)
        if self._subscribed:
            for device_id, device in self._devices.items():
                self._subscriptions[device_id].append(device.alert_raised.subscribe(handler))

    def add_status_handler(self, handler: StatusHandler) -> None:
        self._ensure_alive()
        if not callable(handler):
            raise TypeError("handler phải là callable")
        if handler in self._status_handlers:
            return
        self._status_handlers.append(handler)
        if self._subscribed:
            for device_id, device in self._devices.items():
                self._subscriptions[device_id].append(device.status_changed.subscribe(handler))

    def subscribe(self) -> None:
        self._ensure_alive()
        if self._subscribed:
            return

        bound: dict[str, list[Subscription]] = {}
        try:
            for device_id, device in self._devices.items():
                bound[device_id] = self._bind(device)
        except Exception:
            for subscriptions in bound.values():
                self._release(subscriptions)
            raise
        self._subscriptions = bound
        self._subscribed = True

    def unsubscribe(self) -> None:
        if not self._subscribed:
            return
        for subscriptions in self._subscriptions.values():
            self._release(subscriptions)
        self._subscriptions = {}
        self._subscribed = False

    def dispose(self) -> None:
        self.unsubscribe()
        self._disposed = True

    def __enter__(self) -> DeviceMonitoringCoordinator:
        self.subscribe()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.dispose()

    def _bind(self, device: TeachingDevice) -> list[Subscription]:
        subscriptions: list[Subscription] = []
        try:
            for handler in self._status_handlers:
                subscriptions.append(device.status_changed.subscribe(handler))
            for handler in self._alert_handlers:
                subscriptions.append(device.alert_raised.subscribe(handler))
        except Exception:
            self._release(subscriptions)
            raise
        return subscriptions

    @staticmethod
    def _release(subscriptions: Iterable[Subscription]) -> None:
        for subscription in subscriptions:
            subscription.dispose()

    def _ensure_alive(self) -> None:
        if self._disposed:
            raise RuntimeError("Coordinator đã được giải phóng")


def build_devices() -> list[TeachingDevice]:
    return [
        ProjectorRemote("PRJ-01", "Epson Remote", 50),
        AirConditionerRemote("AC-01", "Daikin Remote", "A2-301"),
        WirelessMicrophone("MIC-01", "Shure Mic"),
    ]


def main() -> None:
    devices = build_devices()
    projector, air_con, mic = devices

    notifier = ConsoleNotificationService()
    logger = DeviceAuditLogger()
    maintenance = MaintenanceService()

    criticals: list[str] = []
    critical_only = FilteredAlertSubscriber(
        lambda a: a.severity == AlertSeverity.CRITICAL,
        lambda a: criticals.append(f"NGHIÊM TRỌNG: {a.device_id} - {a.message}"),
    )

    with DeviceMonitoringCoordinator(
        devices,
        notifier,
        logger,
        maintenance,
        extra_alert_handlers=[critical_only.handle_alert],
    ) as monitoring:
        projector.change_status(DeviceStatus.BORROWED)
        projector.change_status(DeviceStatus.BORROWED)
        projector.change_status(DeviceStatus.UNDER_MAINTENANCE)
        projector.update_battery_level(15)
        projector.update_battery_level(10)
        projector.update_battery_level(0)
        projector.update_battery_level(0)

        air_con.update_response_state(False)
        air_con.update_response_state(False)
        air_con.update_response_state(True)
        air_con.update_response_state(False)

        mic.update_signal_quality(40)
        mic.update_signal_quality(39)
        mic.update_battery_level(25)
        try:
            mic.update_battery_level(150)
        except InvalidDeviceReadingException as ex:
            print(f"Bị từ chối: {ex.device_id} -> {ex.invalid_value}")
        print("Pin micro sau khi bị từ chối:", mic.battery_level)

        new_mic = WirelessMicrophone("MIC-02", "Sony Mic")
        monitoring.add_device(new_mic)
        new_mic.update_battery_level(0)

        monitoring.remove_device("MIC-01")
        mic.update_battery_level(0)

    print("\nNhật ký thay đổi trạng thái:", *logger.entries, sep="\n  ")
    print("\nYêu cầu bảo trì:", *maintenance.requests, sep="\n  ")
    print("\nSubscriber chỉ nhận mức nghiêm trọng:", *criticals, sep="\n  ")
    print(
        "\nSố handler còn lại sau khi thoát:",
        [d.alert_raised.handler_count + d.status_changed.handler_count for d in (*devices, new_mic)],
    )


if __name__ == "__main__":
    main()