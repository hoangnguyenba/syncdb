{ pkgs, ... }: {
  channel = "stable-24.11";  # or use a newer stable channel if you want
  packages = [
    pkgs.go
    pkgs.gnumake
  ];
}
