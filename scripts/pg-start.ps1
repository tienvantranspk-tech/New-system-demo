$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$bin  = Join-Path $root '.pgsql\pgsql\bin'
$data = Join-Path $root '.pgdata'
$log  = Join-Path $root '.pgdata\server.log'
& (Join-Path $bin 'pg_ctl.exe') -D $data -l $log start
