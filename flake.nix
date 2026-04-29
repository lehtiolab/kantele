{
  description = "Kantele Dev Shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Backend & Tooling
            python311
            uv
            # Matches Kantele README reqs
            nodejs_22
            npm
            
            # Infrastructure
            docker
            docker-compose
            postgresql_18 # For local CLI access to the DB container
            
            # Quality of Life
            git
            pre-commit
            ruff # Fast Python linting for the Django code
          ];

          shellHook = ''
            echo "--- Kantele Development Environment ---"
          '';
        };
      });
}