# Setup guide

How to get a machine ready to run the project, on macOS, Ubuntu and Windows. Every system ends
in the same place: a terminal where `docker`, `make`, `uv`, `java` (21) and `node` (24) work.
After that the [README quickstart](../README.md#quickstart) is identical on all of them.

**What was tested.** The macOS steps were run on an Apple Silicon Mac. The Ubuntu and Windows
steps are written from the vendors' own documentation (Adoptium, Docker, Microsoft, checked on
2026-10-27) and were not run on those systems. If a step does not work for you, fix it here.

## Pick your path

| You use | Go to |
|---------|-------|
| macOS 13 or newer, Apple Silicon or Intel | [macOS](#macos) |
| Ubuntu 22.04 or 24.04 | [Ubuntu](#ubuntu) |
| Windows 10 (version 2004 or newer) or Windows 11 | [Windows, through WSL2](#windows-through-wsl2). You will work inside Ubuntu, so you follow the Ubuntu section too |

Windows runs the project through **WSL2** (Windows Subsystem for Linux) because the project is
built on `make`, bash scripts and Docker. Plain PowerShell is not supported.

What gets installed:

| Tool | Version | Why |
|------|---------|-----|
| git, make | any recent | version control, the `make` targets |
| Docker with Compose | recent, v2 or newer | runs Kafka, Connect, Cassandra, HDFS and the rest |
| [uv](https://docs.astral.sh/uv/) | any recent version | Python projects and tools. It downloads Python 3.12 itself, so do not install Python for the project |
| Java (Temurin) | 21 | the Spark tests. Spark 4.2 also runs on 17 |
| Node | 24 LTS | the dashboard |

## macOS

1. Command line tools (gives you `git` and `make`):

   ```bash
   xcode-select --install
   ```

2. Docker. Install **one** of these and start it once:

   ```bash
   brew install --cask docker       # Docker Desktop
   brew install --cask orbstack     # or OrbStack, lighter and faster on Apple Silicon
   ```

   No Homebrew yet? See <https://brew.sh>.

3. uv:

   ```bash
   brew install uv
   ```

4. Java 21:

   ```bash
   brew install --cask temurin@21
   export JAVA_HOME=$(/usr/libexec/java_home -v 21)
   ```

   Add the `export` line to `~/.zshrc` to keep it. If an older Java is already set in your shell,
   `make test` ignores it and looks for a newer one, but setting it yourself is clearer.

5. Node: see [Node](#node-macos-ubuntu-and-wsl).

Then run [Check your setup](#check-your-setup).

## Ubuntu

For Ubuntu 22.04 and 24.04.

1. Basic tools. `python3` is used by the helper scripts, `zip` by `make spark-package`:

   ```bash
   sudo apt update
   sudo apt install -y git make curl zip unzip python3
   ```

2. **Docker Engine.** Follow "Install using the apt repository" on
   <https://docs.docker.com/engine/install/ubuntu/> (the exact commands change over time, so they
   are not copied here). Then let your user run Docker without `sudo`:

   ```bash
   sudo usermod -aG docker $USER
   ```

   Log out and back in, then check:

   ```bash
   docker compose version        # prints a version, v2 or newer
   docker run --rm hello-world
   ```

3. uv:

   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   Open a new terminal afterwards so `uv` is on your `PATH`.

4. Java 21 (Eclipse Temurin), from the Adoptium apt repository:

   ```bash
   sudo apt install -y wget apt-transport-https gpg
   wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/adoptium.gpg > /dev/null
   echo "deb https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | sudo tee /etc/apt/sources.list.d/adoptium.list
   sudo apt update
   sudo apt install -y temurin-21-jdk
   ```

   On Ubuntu 24.04 `sudo apt install openjdk-21-jdk` also works; Spark does not care which
   Java 21 you use. Ubuntu has no `java_home` tool, so tell the shell where Java is. If you have
   several Javas, pick 21 first with `sudo update-alternatives --config java`.

   ```bash
   export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
   echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
   java -version     # must say 21
   ```

5. Node: see [Node](#node-macos-ubuntu-and-wsl).

Then run [Check your setup](#check-your-setup).

## Windows, through WSL2

You install two things on Windows (WSL and Docker Desktop) and do everything else inside the
Ubuntu terminal.

**A. WSL with Ubuntu.** Open PowerShell **as administrator** and run:

```powershell
wsl --install
```

Restart when asked. Open **Ubuntu** from the Start menu, wait for the first-run setup, and create
a Linux username and password. Check that it runs as WSL 2:

```powershell
wsl -l -v          # the VERSION column must say 2
```

**B. Docker Desktop.** Install it with `winget` (or from docker.com):

```powershell
winget install -e --id Docker.DockerDesktop
```

Start Docker Desktop, then check two settings:

- Settings > General: "Use the WSL 2 based engine" is ticked.
- Settings > Resources > WSL Integration: Ubuntu is switched on.

Do **not** install Docker Engine inside Ubuntu as well. Two engines conflict.

**C. Everything else, in the Ubuntu terminal.** Follow the [Ubuntu](#ubuntu) section, skipping
step 2 (Docker Engine), because Docker Desktop provides it. Check that it reaches Docker:

```bash
docker compose version
```

**D. Keep the project in the Linux home folder.** Clone into `~/` (for example
`~/projects`), not under `/mnt/c/...`. Files on the Windows side are slow for Docker and Spark, and
Windows Git can turn line endings into CRLF, which breaks the shell scripts. The repo's
`.gitattributes` keeps them as LF, but a clone made inside Ubuntu avoids the problem completely.

```bash
mkdir -p ~/projects && cd ~/projects
git clone <repository-url>
```

**E. Optional but useful.**

- Editor: install VS Code with the "WSL" extension, then run `code .` inside the project folder
  in the Ubuntu terminal.
- Memory: the full stack is heavy. WSL takes up to half of your RAM by default. To give it more,
  create `%UserProfile%\.wslconfig` (that is `C:\Users\<you>\.wslconfig`) and run
  `wsl --shutdown` afterwards:

  ```ini
  [wsl2]
  memory=10GB
  processors=4
  ```

Web pages the stack serves (for example <http://localhost:8085>) open in your normal Windows
browser.

## Node (macOS, Ubuntu and WSL)

Node is installed with `nvm`, so the project can pin the version in
`services/dashboard/.nvmrc`:

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.8/install.sh | bash
```

Open a new terminal, then:

```bash
cd services/dashboard
nvm install        # reads .nvmrc and installs Node 24
node --version     # v24.x
```

## Check your setup

Run these in the terminal you will use (the Ubuntu terminal on Windows). Each must work:

```bash
git --version
make --version
docker compose version        # v2 or newer
docker run --rm hello-world   # prints "Hello from Docker!"
uv --version
java -version                 # openjdk version "21..."
node --version                # v24.x
```

Then continue with the [README quickstart](../README.md#quickstart): copy `.env.example` to
`.env`, add your aisstream key, `make up-core`, `make topics schemas`.

## Resources

The full stack (Kafka, Connect, Cassandra, HDFS, Spark and the web tier) is heavy. As a rule of
thumb (not measured) give Docker at least 8 GB of memory. `make up-core` on its own needs much
less, so start there and add profiles as you need them.

## Common problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| `make: command not found` | build tools missing | macOS: `xcode-select --install`. Ubuntu: `sudo apt install make` |
| `permission denied ... /var/run/docker.sock` (Ubuntu) | your user is not in the `docker` group | `sudo usermod -aG docker $USER`, then log out and in |
| `Cannot connect to the Docker daemon` (macOS, Windows) | Docker Desktop or OrbStack is not running | start it. On Windows also check Settings > Resources > WSL Integration |
| `docker: command not found` inside Ubuntu on Windows | WSL integration is off | switch Ubuntu on in Docker Desktop, then reopen the Ubuntu terminal |
| `bash\r: No such file or directory`, or `$'\r': command not found` | the files have Windows (CRLF) line endings | clone again inside Ubuntu. Quick fix: `sed -i 's/\r$//' scripts/*.sh` |
| Spark tests fail with "Spark needs Java 17+ (found 11)" | `JAVA_HOME` points to an old Java | set `JAVA_HOME` as in your OS section and check `java -version` |
| `make test` warns that Java is not 21 | Java 17 is installed instead | works, but install 21 to match the team |
| `uv: command not found` or `nvm: command not found` right after installing | the new `PATH` is not loaded yet | open a new terminal |
| `port is already allocated` | another program uses 8080, 8081, 8085, 9092 or similar | stop that program, or `make down` if it is an old run of this stack |
| `namenode` is slow or fails to start on an ARM machine | the Hadoop image only exists for amd64, so it runs emulated | wait longer; on Apple Silicon OrbStack or Docker Desktop's Rosetta setting helps |
| `make download-dma` cannot write to `data/dma` (Linux) | Docker created the folder as root | `sudo chown -R $USER data`. `make up` now creates the folder first, so this only affects old runs |
| Everything is slow on Windows | the project is under `/mnt/c` | move it into the Linux home folder |
