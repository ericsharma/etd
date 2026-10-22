{
  description = "etd: reach tmux on any machine from E.T. over iroh";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
    in
    {
      packages = forAll (pkgs: rec {
        etd = pkgs.rustPlatform.buildRustPackage {
          pname = "etd";
          version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
          src = self;
          cargoLock = {
            lockFile = ./Cargo.lock;
            # A dev-dependency (crates/etd tests), but Cargo.lock vendors it too.
            outputHashes."manymux-0.2.2" = "sha256-gCK68CXHvz5MhRBomft6doBvRIwhAv4blezmknXZWZw=";
          };
          cargoBuildFlags = [
            "-p"
            "etd"
          ];
          # The end-to-end test needs tmux and loopback networking; unit tests only.
          cargoTestFlags = [
            "--workspace"
            "--lib"
          ];
          meta = {
            description = "Reach tmux on this machine from E.T. over iroh";
            license = with pkgs.lib.licenses; [
              mit
              asl20
            ];
            mainProgram = "etd";
          };
        };
        default = etd;
      });

      # Home Manager: `services.etd.enable = true;` runs etd as a user service
      # (systemd on Linux, launchd on macOS). Pair a device with `etd pair`.
      homeManagerModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.etd;
          exe = lib.getExe cfg.package;
        in
        {
          options.services.etd = {
            enable = lib.mkEnableOption "etd, which lets E.T. reach tmux on this machine over iroh";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.etd;
              description = "The etd package.";
            };
          };

          config = lib.mkIf cfg.enable (
            lib.mkMerge [
              { home.packages = [ cfg.package ]; }
              (lib.mkIf pkgs.stdenv.hostPlatform.isLinux {
                systemd.user.services.etd = {
                  Unit.Description = "etd: reach tmux on this machine from E.T. over iroh";
                  Service = {
                    ExecStart = exe;
                    Restart = "always";
                    RestartSec = 2;
                  };
                  Install.WantedBy = [ "default.target" ];
                };
              })
              (lib.mkIf pkgs.stdenv.hostPlatform.isDarwin {
                launchd.agents.etd = {
                  enable = true;
                  config = {
                    ProgramArguments = [ exe ];
                    RunAtLoad = true;
                    KeepAlive = true;
                    StandardOutPath = "${config.home.homeDirectory}/Library/Logs/etd.log";
                    StandardErrorPath = "${config.home.homeDirectory}/Library/Logs/etd.log";
                  };
                };
              })
            ]
          );
        };

      devShells = forAll (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.rustc
            pkgs.cargo
            pkgs.clippy
            pkgs.rustfmt
            pkgs.gcc
            pkgs.tmux
          ];
          RUST_BACKTRACE = "1";
        };
      });
    };
}
