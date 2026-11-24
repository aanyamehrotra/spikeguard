#Requires -Version 5.1
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $false

function Get-LauncherSetting([string]$Name, [string]$Default) {
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrEmpty($value)) {
        return $Default
    }
    return $value
}

$server = $env:LLAMA_SERVER_PATH
if ($server) {
    if (-not (Test-Path -LiteralPath $server -PathType Leaf)) {
        throw "LLAMA_SERVER_PATH does not point to a file: $server"
    }
    $server = (Resolve-Path -LiteralPath $server).ProviderPath
} else {
    $command = Get-Command -Name 'llama-server.exe' -CommandType Application -ErrorAction SilentlyContinue
    if (-not $command) {
        throw 'llama-server.exe was not found. Set LLAMA_SERVER_PATH or add the executable to PATH.'
    }
    $server = $command.Source
}

$modelArgs = if ($env:LLAMA_MODEL_PATH) {
    '--model', $env:LLAMA_MODEL_PATH
} else {
    '--hf-repo', (Get-LauncherSetting 'LLAMA_MODEL_REPO' 'Qwen/Qwen2.5-0.5B-Instruct-GGUF:Q4_K_M')
}

$serverArgs = @(
    '--host', '127.0.0.1'
    '--port', (Get-LauncherSetting 'PORT' '8080')
    $modelArgs
    '--alias', 'spikeguard'
    '--parallel', (Get-LauncherSetting 'SLOTS' '2')
    '--ctx-size', (Get-LauncherSetting 'CONTEXT' '4096')
    '--threads', (Get-LauncherSetting 'LLAMA_THREADS' '2')
    '--cache-ram', (Get-LauncherSetting 'LLAMA_CACHE_MIB' '128')
    '--n-gpu-layers', (Get-LauncherSetting 'LLAMA_GPU_LAYERS' 'auto')
    '--cont-batching', '--metrics'
)

& $server @serverArgs
exit $LASTEXITCODE
