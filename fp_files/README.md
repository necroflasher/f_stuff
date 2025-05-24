these are the file lists of all* downloads on the "archived flash player versions" download page ([last capture](https://web.archive.org/web/20200718192527/https://helpx.adobe.com/flash-player/kb/archived-flash-player-versions.html))

- `all.list` contains a line for each zip archive and file in it

- the json files contain more detailed info about each archive:
  - `files[].name`: name from the zip file entry
  - `files[].offset`: byte offset of the zip file entry
  - `files[].size`: uncompressed size of the file
  - `modified`: http last-modified header of the archive
  - `size`: size in bytes of the archive
  - `url`: link to the archive

\* archives newer than `fp_32.0.0.371_archive.zip` are missing. if needed, a list of versions can be found on [wikipedia](https://en.wikipedia.org/wiki/Adobe_Flash_Player#Release_history)
