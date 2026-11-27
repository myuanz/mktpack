from pathlib import Path
import shutil
import subprocess


def test_cli_skill_stdout_and_install(tmp_path):
    mkt = shutil.which('mkt')
    printed = subprocess.run([mkt, 'skill'], capture_output=True, text=True, encoding='utf-8', check=True)
    assert printed.stdout.startswith('---\nname: mktpack\n')
    assert 'mktpack.open' in printed.stdout

    skills_root = tmp_path / 'skills'
    installed = subprocess.run([mkt, 'skill', '--install', str(skills_root)],
                               capture_output=True, text=True, encoding='utf-8', check=True)
    target = skills_root / 'mktpack' / 'SKILL.md'
    assert Path(installed.stdout.strip()) == target
    assert target.read_text(encoding='utf-8') == printed.stdout

    # 传入 mktpack 目录本身时不重复嵌套
    nested = subprocess.run([mkt, 'skill', '--install', str(skills_root / 'mktpack')],
                            capture_output=True, text=True, encoding='utf-8', check=True)
    assert Path(nested.stdout.strip()) == target

    # 直接传入 SKILL.md 文件路径
    direct = tmp_path / 'custom' / 'SKILL.md'
    written = subprocess.run([mkt, 'skill', '--install', str(direct)],
                             capture_output=True, text=True, encoding='utf-8', check=True)
    assert Path(written.stdout.strip()) == direct
    assert direct.read_text(encoding='utf-8') == printed.stdout
