{
  description = "etd: reach tmux on any machine from E.T. over iroh";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    # The manymux node that etd installs next to itself (one pinned release;
    # crates/etd and crates/et-link pin the same commit in Cargo.toml).
    manymux = {
      url = "github:rayfish/manymux/v0.2.2";
      flake = false;
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      manymux,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
        "x86_64-darwin"
      ];
      forAll = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      # Not `manymux` inside `packages`: the package of that name would shadow it.
      manymuxSrc = manymux;
    in
    {
      packages = forAll (pkgs: rec {
        etd = pkgs.rustPlatform.buildRustPackage {
          pname = "etd";
          version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
          src = self;
          cargoLock = {
            lockFile = ./Cargo.lock;
            # manymux, a git dependency: etd (events) and et-link use its client core.
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

        # `mm`: persistent terminal sessions (https://github.com/rayfish/manymux).
        manymux = pkgs.rustPlatform.buildRustPackage {
          pname = "manymux";
          version = (pkgs.lib.importTOML "${manymuxSrc}/Cargo.toml").package.version;
          src = manymuxSrc;
          cargoLock.lockFile = "${manymuxSrc}/Cargo.lock";
          # build.rs reads the commit from git, which the Nix sandbox does not have.
          env.MM_COMMIT = builtins.substring 0 8 manymuxSrc.rev;
          cargoBuildFlags = [
            "--bin"
            "mm"
          ];
          # Its end-to-end tests drive a stub ssh and real PTYs; upstream CI runs them.
          doCheck = false;
          meta = {
            description = "Persistent terminal sessions across machines";
            homepage = "https://github.com/rayfish/manymux";
            license = pkgs.lib.licenses.mpl20;
            mainProgram = "mm";
          };
        };

        default = etd;
      });

      # Home Manager: `services.etd.enable = true;` runs etd as a user service
      # (systemd on Linux, launchd on macOS), and next to it the manymux node
      # with MM_SSH=etd-ssh, so `mm` reaches every machine of the network.
      # Add a device with `etd invite`, a machine with `etd invite --machine`.
      homeManagerModules.default =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.etd;
          system = pkgs.stdenv.hostPlatform.system;
          exe = lib.getExe cfg.package;
          mm = lib.getExe cfg.manymux.package;
          etdSsh = "${cfg.package}/bin/etd-ssh";
          logs = "${config.home.homeDirectory}/Library/Logs";
          etdEnv =
            lib.optionalAttrs (cfg.manymux.enable && cfg.manymux.manageHosts) { ETD_MANYMUX_HOSTS = "1"; }
            // lib.optionalAttrs (cfg.notifyCommand != null) { ETD_NOTIFY_COMMAND = cfg.notifyCommand; };
          # systemd splits Environment= on spaces unless each assignment is quoted.
          systemdEnv = lib.mapAttrsToList (k: v: ''"${k}=${v}"'') etdEnv;
        in
        {
          options.services.etd = {
            enable = lib.mkEnableOption "etd, which lets E.T. reach this machine over iroh";
            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${system}.etd;
              description = "The etd package.";
            };
            notifyCommand = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              default = null;
              description = "Run for a bell in a manymux session nobody saw (ETD_NOTIFY_COMMAND; see docs/PROTOCOL.md).";
            };
            manymux = {
              enable = lib.mkOption {
                type = lib.types.bool;
                default = true;
                description = "Also run the manymux node (`mm daemon`), with MM_SSH=etd-ssh for it and for your shells.";
              };
              package = lib.mkOption {
                type = lib.types.package;
                default = self.packages.${system}.manymux;
                description = "The manymux package.";
              };
              manageHosts = lib.mkOption {
                type = lib.types.bool;
                default = true;
                description = "Keep manymux's machine list (hosts.toml) equal to the network's machines.";
              };
            };
          };

          config = lib.mkIf cfg.enable (
            lib.mkMerge [
              { home.packages = [ cfg.package ]; }
              (lib.mkIf pkgs.stdenv.hostPlatform.isLinux {
                systemd.user.services.etd = {
                  Unit.Description = "etd: reach this machine from E.T. over iroh";
                  Service = {
                    ExecStart = exe;
                    Restart = "always";
                    RestartSec = 2;
                    Environment = systemdEnv;
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
                    EnvironmentVariables = etdEnv;
                    StandardOutPath = "${logs}/etd.log";
                    StandardErrorPath = "${logs}/etd.log";
                  };
                };
              })

              (lib.mkIf cfg.manymux.enable (
                lib.mkMerge [
                  {
                    home.packages = [ cfg.manymux.package ];
                    # Your own `mm ls` / `mm attach` reach other machines through etd (D5).
                    home.sessionVariables.MM_SSH = etdSsh;
                  }
                  (lib.mkIf pkgs.stdenv.hostPlatform.isLinux {
                    systemd.user.services.manymux = {
                      Unit = {
                        Description = "manymux node: persistent terminal sessions";
                        # A restart ends every session on this machine. Never on a
                        # deploy; pick up a new mm by hand (`mm update --keep-sessions`).
                        X-RestartIfChanged = false;
                      };
                      Service = {
                        # --no-notify: etd sends bells on (ETD_NOTIFY_COMMAND).
                        ExecStart = "${mm} daemon --no-notify";
                        Environment = [ "MM_SSH=${etdSsh}" ];
                        Restart = "on-failure";
                        RestartSec = 2;
                      };
                      Install.WantedBy = [ "default.target" ];
                    };
                  })
                  (lib.mkIf pkgs.stdenv.hostPlatform.isDarwin {
                    launchd.agents.manymux = {
                      enable = true;
                      config = {
                        ProgramArguments = [
                          mm
                          "daemon"
                          "--no-notify"
                        ];
                        EnvironmentVariables.MM_SSH = etdSsh;
                        RunAtLoad = true;
                        KeepAlive = true;
                        StandardErrorPath = "${logs}/manymux-daemon.log";
                      };
                    };
                  })
                ]
              ))
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
            # The manymux tests need `mm` (they run their own node).
            self.packages.${pkgs.stdenv.hostPlatform.system}.manymux
          ];
          RUST_BACKTRACE = "1";
        };
      });
    };
}
