import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'serve.sh'
SETTINGS = {
    'PORT', 'SLOTS', 'CONTEXT', 'LLAMA_MODEL_REPO', 'LLAMA_MODEL_PATH',
    'LLAMA_THREADS', 'LLAMA_CACHE_MIB', 'LLAMA_GPU_LAYERS',
}


@unittest.skipUnless(os.name == 'posix' and shutil.which('bash'), 'native launcher uses Bash on macOS and Linux')
class LauncherTests(unittest.TestCase):
    def launch(self, **settings):
        env = {key: value for key, value in os.environ.items() if key not in SETTINGS}
        with tempfile.TemporaryDirectory() as directory:
            stub = Path(directory, 'llama-server')
            stub.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$@"\n', encoding='utf-8')
            stub.chmod(0o755)
            env.update(settings, PATH=directory + os.pathsep + env['PATH'])
            result = subprocess.run(
                ['bash', str(SCRIPT)], env=env, check=True, capture_output=True, text=True, timeout=5,
            )
        return result.stdout.splitlines()

    def assert_option(self, args, option, value):
        self.assertEqual(args[args.index(option) + 1], value)

    def test_defaults_use_a_small_model_and_bounded_resources(self):
        args = self.launch()

        self.assert_option(args, '--host', '127.0.0.1')
        self.assert_option(args, '--hf-repo', 'Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M')
        self.assert_option(args, '--parallel', '2')
        self.assert_option(args, '--ctx-size', '4096')
        self.assert_option(args, '--threads', '2')
        self.assert_option(args, '--cache-ram', '128')
        self.assert_option(args, '--n-gpu-layers', 'auto')

    def test_local_model_and_cpu_overrides(self):
        args = self.launch(
            LLAMA_MODEL_PATH='/tmp/local model.gguf', LLAMA_GPU_LAYERS='0', LLAMA_THREADS='1',
            LLAMA_CACHE_MIB='0', SLOTS='1', CONTEXT='2048', PORT='18080',
        )

        self.assertNotIn('--hf-repo', args)
        self.assert_option(args, '--model', '/tmp/local model.gguf')
        self.assert_option(args, '--n-gpu-layers', '0')
        self.assert_option(args, '--threads', '1')
        self.assert_option(args, '--cache-ram', '0')
        self.assert_option(args, '--parallel', '1')
        self.assert_option(args, '--ctx-size', '2048')
        self.assert_option(args, '--port', '18080')
