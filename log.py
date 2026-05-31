"""Общий логгер. Когда активен Prompt — пишет через него, не ломая строку ввода.
До инициализации Prompt падает в обычный print."""

_prompt = None


def attach(prompt):
    global _prompt
    _prompt = prompt


def log(msg):
    p = _prompt
    if p is not None:
        p.print(msg)
    else:
        print(msg)
