from __future__ import annotations

import sys
import threading
import time


class ProgressBar:
    """Dependency-free, single-line terminal progress bar.

    Draws only when the target stream is a real TTY (``mode="auto"``, the
    default), so redirected/nohup/CI output is never polluted with partial
    ``\\r`` lines; in that case ``update``/``close`` become cheap no-ops and
    callers should rely on the stage's normal periodic log lines instead.
    Pass ``mode=True``/``mode=False`` to force enable/disable.
    """

    def __init__(
        self,
        total: int | None,
        desc: str = "",
        mode: str | bool = "auto",
        stream=None,
        min_interval: float = 0.2,
        bar_width: int = 30,
    ):
        self.total = total
        self.desc = desc
        self.stream = stream or sys.stderr
        self.min_interval = min_interval
        self.bar_width = bar_width
        self.count = 0
        self.postfix: dict = {}
        self.start_time = time.monotonic()
        self._last_draw_time = 0.0
        self._lock = threading.Lock()
        self._closed = False
        if mode == "auto":
            self.active = bool(getattr(self.stream, "isatty", lambda: False)())
        else:
            self.active = bool(mode)

    def update(self, n: int = 1, **postfix) -> None:
        if not self.active:
            self.count += n
            return
        with self._lock:
            self.count += n
            self.postfix.update(postfix)
            now = time.monotonic()
            is_last = self.total is not None and self.count >= self.total
            if is_last or now - self._last_draw_time >= self.min_interval:
                self._draw(now)
                self._last_draw_time = now

    def _draw(self, now: float) -> None:
        elapsed = max(now - self.start_time, 1e-9)
        rate = self.count / elapsed
        postfix_text = " ".join(f"{key}={value}" for key, value in self.postfix.items())
        if self.total:
            fraction = min(self.count / self.total, 1.0)
            filled = int(self.bar_width * fraction)
            bar = "█" * filled + "░" * (self.bar_width - filled)
            remaining = max(self.total - self.count, 0)
            eta = remaining / rate if rate > 0 else 0
            line = (
                f"\r{self.desc} [{bar}] {self.count}/{self.total} "
                f"({fraction * 100:5.1f}%) {rate:6.1f}张/秒 剩余{_format_duration(eta)}"
            )
        else:
            line = f"\r{self.desc} 已处理 {self.count} 条 {rate:6.1f}条/秒 用时{_format_duration(elapsed)}"
        if postfix_text:
            line += f" | {postfix_text}"
        self.stream.write(line)
        self.stream.flush()

    def close(self) -> None:
        with self._lock:
            if self._closed or not self.active:
                self._closed = True
                return
            self._closed = True
            self._draw(time.monotonic())
            self.stream.write("\n")
            self.stream.flush()


def _format_duration(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}时{minutes}分"
    if minutes:
        return f"{minutes}分{secs}秒"
    return f"{secs}秒"
