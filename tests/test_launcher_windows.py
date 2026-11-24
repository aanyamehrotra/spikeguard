import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'serve.ps1'
POWERSHELL = shutil.which('powershell.exe') or shutil.which('pwsh.exe')
SETTINGS = {
    'PORT', 'SLOTS', 'CONTEXT', 'LLAMA_SERVER_PATH', 'LLAMA_MODEL_REPO', 'LLAMA_MODEL_PATH',
    'LLAMA_THREADS', 'LLAMA_CACHE_MIB', 'LLAMA_GPU_LAYERS',
}


@unittest.skipUnless(os.name == 'nt' and POWERSHELL, 'Windows PowerShell launcher')
class WindowsLauncherTests(unittest.TestCase):
    def launch(self, exit_code=0, **settings):
        env = {key: value for key, value in os.environ.items() if key not in SETTINGS}
        with tempfile.NamedTemporaryFile(
            mode='w', encoding='ascii', prefix='llama server stub ', suffix='.cmd', delete=False,
        ) as stub:
            stub.write(
                '@echo off\n'
                ':arg\n'
                f'if "%~1"=="" exit /b {exit_code}\n'
                'echo(%~1\n'
                'shift\n'
                'goto arg\n',
            )
        try:
            env.update(settings, LLAMA_SERVER_PATH=stub.name)
            return subprocess.run(
                [POWERSHELL, '-NoProfile', '-ExecutionPolicy', 'RemoteSigned', '-File', str(SCRIPT)],
                env=env, capture_output=True, text=True, timeout=15,
            )
        finally:
            Path(stub.name).unlink()

    def test_defaults_match_the_native_baseline(self):
        result = self.launch()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [
            '--host', '127.0.0.1', '--port', '8080',
            '--hf-repo', 'Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M', '--alias', 'spikeguard',
            '--parallel', '2', '--ctx-size', '4096', '--threads', '2', '--cache-ram', '128',
            '--n-gpu-layers', 'auto', '--cont-batching', '--metrics',
        ])

    def test_local_paths_and_resource_overrides_preserve_native_exit_status(self):
        model = r'C:\models with spaces\local model.gguf'
        result = self.launch(
            exit_code=7, LLAMA_MODEL_PATH=model, LLAMA_MODEL_REPO='ignored/model',
            PORT='18080', SLOTS='1', CONTEXT='2048', LLAMA_THREADS='1', LLAMA_CACHE_MIB='0', LLAMA_GPU_LAYERS='0',
        )
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(result.stdout.splitlines(), [
            '--host', '127.0.0.1', '--port', '18080', '--model', model, '--alias', 'spikeguard',
            '--parallel', '1', '--ctx-size', '2048', '--threads', '1', '--cache-ram', '0',
            '--n-gpu-layers', '0', '--cont-batching', '--metrics',
        ])
