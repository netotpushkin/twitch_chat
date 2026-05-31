"""Терминальный ввод, который не ломается приходящими сообщениями."""

import _thread
import os
import sys
import threading

if sys.platform == "win32":
    os.system("")
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError, OSError):
        # stdout перенаправлен/запайплен — оставляем как есть.
        pass
    try:
        import msvcrt
    except ImportError:
        msvcrt = None
else:
    msvcrt = None


class Prompt:
    """Свой посимвольный ввод. Все исходящие в stdout прогоняются через .print(),
    которая стирает строку ввода, печатает сообщение «выше», и возвращает курсор
    обратно в строку набора."""
    PROMPT = "> "

    def __init__(self):
        self.buf = ""
        self.lock = threading.Lock()
        self.active = msvcrt is not None  # на не-Windows откатываемся к input()

    def start(self, on_submit):
        if not self.active:
            def fallback():
                while True:
                    try:
                        line = input()
                    except EOFError:
                        return
                    if line.strip():
                        on_submit(line.strip())
            threading.Thread(target=fallback, daemon=True).start()
            return

        def loop():
            sys.stdout.write(self.PROMPT); sys.stdout.flush()
            while True:
                ch = msvcrt.getwch()
                with self.lock:
                    if ch == "\x03":  # Ctrl+C
                        _thread.interrupt_main()
                        return
                    if ch in ("\r", "\n"):
                        line = self.buf
                        self.buf = ""
                        sys.stdout.write("\r\x1b[K"); sys.stdout.flush()
                        sys.stdout.write(self.PROMPT); sys.stdout.flush()
                    elif ch == "\x08":  # Backspace
                        if self.buf:
                            self.buf = self.buf[:-1]
                            sys.stdout.write("\b \b"); sys.stdout.flush()
                        continue
                    elif ch in ("\x00", "\xe0"):  # префикс спецкл. (стрелки/F-ки) — игнор
                        msvcrt.getwch()
                        continue
                    else:
                        self.buf += ch
                        sys.stdout.write(ch); sys.stdout.flush()
                        continue
                # Enter попал сюда — line уже снят, lock отпущен
                if line.strip():
                    on_submit(line.strip())

        threading.Thread(target=loop, daemon=True).start()

    def print(self, text):
        """Печать поверх строки ввода без её затирания."""
        if not self.active:
            print(text)
            return
        with self.lock:
            sys.stdout.write("\r\x1b[K" + text + "\n" + self.PROMPT + self.buf)
            sys.stdout.flush()
