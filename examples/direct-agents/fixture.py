"""Synthetic source and eight fixed behavioral cases for an agent exercise."""
BASE = {'quote.py': "def quote():\n    return {'minor': 1250, 'currency': 'USD'}\n",
 'invoice.py': "def invoice():\n    return 'USD 12.50'\n",
 'ledger.py': "def ledger():\n    return ('USD', 1250)\n",
 'shipping.py': 'def fee(units):\n'
                '    total = 0\n'
                '    for _ in range(units):\n'
                '        total += 3\n'
                '    return total\n',
 'currency.py': "def upper(text):\n    return ''.join(char.upper() for char in text)\n",
 'totals.py': 'def total(values):\n'
              '    result = 0\n'
              '    for value in values:\n'
              '        result += value\n'
              '    return result\n',
 'positive.py': 'def positive(values):\n'
                '    result = []\n'
                '    for value in values:\n'
                '        if value > 0:\n'
                '            result.append(value)\n'
                '    return result\n',
 'trim.py': 'def trim(text):\n'
            "    while text.startswith(' '):\n"
            '        text = text[1:]\n'
            "    while text.endswith(' '):\n"
            '        text = text[:-1]\n'
            '    return text\n',
 'contract.py': '"""Eight fixed behavioral cases; output TAP and exact source hashes."""\n'
                'if not __debug__:\n'
                "    raise SystemExit('Optimized Python unsupported')\n"
                'import argparse\n'
                'import hashlib\n'
                'import importlib\n'
                'import json\n'
                'from pathlib import Path\n'
                'import time\n'
                '\n'
                'parser = argparse.ArgumentParser()\n'
                "parser.add_argument('--barrier', type=Path)\n"
                'args = parser.parse_args()\n'
                'if args.barrier:\n'
                '    assert args.barrier.is_absolute()\n'
                "    args.barrier.with_suffix('.started').write_text(str(time.time()))\n"
                '    deadline = time.monotonic() + 25\n'
                '    while not args.barrier.exists():\n'
                '        if time.monotonic() >= deadline:\n'
                "            raise SystemExit('fixture barrier timeout')\n"
                '        time.sleep(0.025)\n'
                '\n'
                'def check_provider():\n'
                "    value = importlib.import_module('quote').quote()\n"
                "    money = value.get('money', value)\n"
                "    assert money['currency'] == 'USD'\n"
                "    assert money.get('minor_units', money.get('minor')) == 1250\n"
                '\n'
                'def check_invoice():\n'
                "    assert importlib.import_module('invoice').invoice() == 'USD 12.50'\n"
                '\n'
                'def check_ledger():\n'
                "    assert importlib.import_module('ledger').ledger() == ('USD', 1250)\n"
                '\n'
                'def check_shipping():\n'
                "    assert [importlib.import_module('shipping').fee(n) for n in [0, 1, 5]] == [0, "
                '3, 15]\n'
                '\n'
                'def check_currency():\n'
                "    assert [importlib.import_module('currency').upper(s) for s in ['', 'usd', "
                "'EuR']] == ['', 'USD', 'EUR']\n"
                '\n'
                'def check_totals():\n'
                "    assert [importlib.import_module('totals').total(v) for v in [[], [1], [1, -3, "
                '5]]] == [0, 1, 3]\n'
                '\n'
                'def check_positive():\n'
                "    assert [importlib.import_module('positive').positive(v) for v in [[], [-1, "
                '0], [-1, 2, 3]]] == [[], [], [2, 3]]\n'
                '\n'
                'def check_trim():\n'
                "    assert [importlib.import_module('trim').trim(s) for s in ['', '   ', ' a b "
                "']] == ['', '', 'a b']\n"
                '\n'
                'checks = [check_provider, check_invoice, check_ledger, check_shipping, '
                'check_currency,\n'
                '          check_totals, check_positive, check_trim]\n'
                "print('TAP version 13', flush=True)\n"
                'results = []\n'
                'for number, check in enumerate(checks, 1):\n'
                '    try:\n'
                '        check()\n'
                '    except Exception as error:\n'
                "        results.append({'name': check.__name__, 'passed': False, 'error_type': "
                'type(error).__name__})\n'
                "        print(f'not ok {number} - {check.__name__}', flush=True)\n"
                '    else:\n'
                "        results.append({'name': check.__name__, 'passed': True})\n"
                "        print(f'ok {number} - {check.__name__}', flush=True)\n"
                "print('1..8', flush=True)\n"
                "print('# tests 8', flush=True)\n"
                "print('# pass '+str(sum(row['passed'] for row in results)), flush=True)\n"
                "print('# fail '+str(sum(not row['passed'] for row in results)), flush=True)\n"
                "print('# cancelled 0', flush=True)\n"
                "print('# skipped 0', flush=True)\n"
                "print('# todo 0', flush=True)\n"
                'source = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in '
                "Path('.').glob('*.py')}\n"
                "Path('result.json').write_text(json.dumps({'cases': results, 'source': source, "
                "'observed_quote': importlib.import_module('quote').quote()}, "
                "sort_keys=True)+'\\n')\n"
                "raise SystemExit(0 if all(row['passed'] for row in results) else 1)\n",
 '.gitignore': '__pycache__/\nresult.json\n'}
