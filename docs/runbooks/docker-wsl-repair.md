# Docker Desktop WSL Repair Notes

日期：2026-06-14

## 现象

Docker Desktop 弹窗：

```text
Docker Desktop distro installation failed
deploying WSL2 distributions
provisioning docker WSL distros: ensuring main distro is deployed:
checking if main distro is up to date:
checking main distro bootstrap version:
open \\wsl$\docker-desktop\etc\wsl_bootstrap_version:
The filename, directory name, or volume label syntax is incorrect.
checking if isocache exists:
CreateFile \\wsl$\docker-desktop-data\isocache\:
The filename, directory name, or volume label syntax is incorrect.
```

本次诊断结果：

- `wsl --version` 可用，WSL 版本为 2.5.9.0。
- `wsl -l -v` 中存在：
  - `docker-desktop`
  - `docker-desktop-data`
  - `ovm-oomol-studio`
- `WSLService` 正在运行。
- `\\wsl$\` 与 `\\wsl.localhost\` 均不可访问。
- `wsl -d docker-desktop -- uname -a` 失败，核心错误为：

```text
C:\Users\yangyang\AppData\Local\Docker\wsl\main\ext4.vhdx
WSL/Service/CreateInstance/MountDisk/HCS/ERROR_FILE_NOT_FOUND
```

判断：

Docker Desktop 的 WSL distro 注册状态和本地 VHDX 文件不一致。也就是说 distro 名称还在，但 Docker Desktop 期望的 `ext4.vhdx` 文件不存在或不可访问。

## 安全修复顺序

### 1. 非破坏性重启

先试这个，不会删除 Docker 数据。

```powershell
wsl --shutdown
```

然后退出 Docker Desktop，再重新打开 Docker Desktop。

如果仍失败，重启 Windows 后再打开 Docker Desktop。

### 2. 确认 Docker 数据是否需要保留

下一步会影响 Docker Desktop 的 images、containers、volumes。

如果你有重要容器数据，先不要执行 unregister。需要先确认是否有可备份的 `docker-desktop-data`。

### 3. 重建 Docker Desktop WSL distros

仅在你确认可以重建 Docker Desktop 数据后执行。

```powershell
wsl --shutdown
wsl --unregister docker-desktop
wsl --unregister docker-desktop-data
```

然后启动 Docker Desktop，让它重新创建两个 distro。

如果 Docker Desktop 仍引用损坏目录，可以手动检查：

```text
C:\Users\yangyang\AppData\Local\Docker\wsl
```

不要直接删除；更稳妥是先改名备份，例如：

```text
wsl -> wsl.backup-20260614
```

然后再启动 Docker Desktop。

## 项目侧临时绕行

在 Docker 修好之前，项目仍可继续：

- API seed mode 可正常运行。
- 前端可正常运行。
- Phase 2 的代码已支持 PostgreSQL/TimescaleDB，但 DB smoke 需要 Docker 或本机 PostgreSQL。

如果暂时不修 Docker，可以安装本机 PostgreSQL + TimescaleDB，使用同一个：

```text
DATABASE_URL=postgresql://trader:trader@localhost:5432/tradingview_local
```

