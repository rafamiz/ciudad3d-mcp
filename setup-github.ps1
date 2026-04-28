# Setup GitHub repo para ciudad3d-mcp
# Uso: en PowerShell, dentro de la carpeta del proyecto:
#   ./setup-github.ps1
#
# Si tenés GitHub CLI (`gh`) instalado y autenticado, te crea el repo solo.
# Si no, te dice exactamente qué pegar.

$ErrorActionPreference = "Stop"

# 1. Verificar git
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: git no está instalado. Instalalo desde https://git-scm.com/download/win" -ForegroundColor Red
    exit 1
}

# 2. Init repo si hace falta
if (-not (Test-Path ".git")) {
    Write-Host "Inicializando repo git..." -ForegroundColor Cyan
    git init -b main | Out-Null
} else {
    Write-Host "Repo git ya existe, sigo." -ForegroundColor Yellow
}

# 3. Asegurar que .env no se commitee
if ((Test-Path ".env") -and -not (Select-String -Path ".gitignore" -Pattern "^\.env$" -Quiet)) {
    Write-Host "Agregando .env al .gitignore..." -ForegroundColor Cyan
    Add-Content .gitignore "`n.env"
}

# 4. Stage + commit
git add .
$staged = git diff --cached --name-only
if ($staged) {
    git -c user.email="rafamizrahi30@gmail.com" -c user.name="rafa" commit -m "ciudad3d chat web v0.1" | Out-Null
    Write-Host "Commit creado." -ForegroundColor Green
} else {
    Write-Host "Nada nuevo para commitear." -ForegroundColor Yellow
}

# 5. Crear remoto
$ghAvailable = Get-Command gh -ErrorAction SilentlyContinue
$repoName = "ciudad3d-mcp"

if ($ghAvailable) {
    Write-Host "GitHub CLI detectado." -ForegroundColor Green
    $prevPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    gh auth status *> $null
    $authed = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prevPref
    if (-not $authed) {
        Write-Host "Necesitás loguearte primero. Corro 'gh auth login'..." -ForegroundColor Yellow
        gh auth login
    }

    $prevPref = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    gh repo view $repoName *> $null
    $repoExists = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prevPref

    if ($repoExists) {
        Write-Host "El repo ya existe en GitHub. Pusheo los cambios..." -ForegroundColor Yellow
        if (-not (git remote | Select-String -Pattern "^origin$" -Quiet)) {
            $url = (gh repo view $repoName --json url -q .url) + ".git"
            git remote add origin $url
        }
        git push -u origin main
    } else {
        Write-Host "Creando repo público en GitHub..." -ForegroundColor Cyan
        gh repo create $repoName --public --source=. --remote=origin --push --description "Asistente urbanístico de CABA - chat web + MCP"
    }

    $url = gh repo view $repoName --json url -q .url
    Write-Host "`nListo. Repo en: $url" -ForegroundColor Green
    Write-Host "Ahora andá a https://railway.app -> New Project -> Deploy from GitHub repo -> '$repoName'" -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "GitHub CLI ('gh') no detectado." -ForegroundColor Yellow
    Write-Host "Opcion A (mas rapida): instalalo y volve a correr este script:" -ForegroundColor White
    Write-Host "  winget install --id GitHub.cli" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Opcion B (manual):" -ForegroundColor White
    Write-Host "  1. Entra a https://github.com/new" -ForegroundColor Gray
    Write-Host "     Repository name: $repoName" -ForegroundColor Gray
    Write-Host "     Public, sin README/gitignore/license (ya los tenes)" -ForegroundColor Gray
    Write-Host "     Click 'Create repository'." -ForegroundColor Gray
    Write-Host ""
    Write-Host "  2. En esta misma terminal, copia y pega (reemplaza TU-USUARIO):" -ForegroundColor Gray
    Write-Host "     git remote add origin https://github.com/TU-USUARIO/$repoName.git" -ForegroundColor Cyan
    Write-Host "     git push -u origin main" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  3. Despues de pushear, anda a https://railway.app y deployalo." -ForegroundColor Gray
}
