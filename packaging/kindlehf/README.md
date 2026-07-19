# kindlehf package staging

`install.sh` installs prebuilt ARM binaries from `payload/` into an inactive A/B slot under `/var/local/kindlebridge`. It never remounts or writes the stock root filesystem.

The current package intentionally installs only an on-demand launcher. A persistent startup adapter must be added only after its lifecycle, OTA behaviour, disable flag, and recovery path have been verified on the target firmware.

`uninstall.sh` removes only KindleBridge-owned files. User application data is retained unless `--purge` is supplied.
