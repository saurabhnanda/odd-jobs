# I used chatgpt to generate this template and then just
# modified to how I normally use these things.
{
  description = "odd jobs";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, flake-compat }:
    let
      pkgs = nixpkgs.legacyPackages.x86_64-linux;
      hpkgs = pkgs.haskellPackages.override {
        overrides = hnew: hold: {
          odd-jobs = pkgs.haskell.lib.dontCheck # TOOD need to figure out how to make it call nixosTest end setup a postgres
            (hnew.callCabal2nix "odd-jobs" ./. { });
          servant-static-th = pkgs.haskell.lib.dontCheck (pkgs.haskell.lib.markUnbroken hold.servant-static-th);
          resource-pool = hnew.callHackageDirect {
              pkg = "resource-pool";
              ver = "0.4.0.0";
              sha256 = "sha256-X3VI1LnkyB28ZumRzOFUNG1UIJiW1UH63ZW/CPqPln4=";
            } {};
        };
      };
    in
    {
      defaultPackage.x86_64-linux =  hpkgs.odd-jobs;
      inherit pkgs;
      devShell.x86_64-linux = hpkgs.shellFor {
        packages = ps : [ ps."odd-jobs" ];
        withHoogle = true;

        buildInputs = [
          hpkgs.haskell-language-server
          pkgs.ghcid
          pkgs.cabal-install
        ];
      };
    };
}
