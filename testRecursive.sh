#!/bin/sh
set -e
recursive_for_loop() {
    ls -1| while read f; do
        if [ -d $f  -a ! -h $f ]; then
            echo "$f"
            if [ $f == "saga" ] || [ $f == "eventstore" ]; then
                continue
            fi
            cd -- "$f"
            if find "." -maxdepth 1 | grep "_test.go"; then
                go test
            fi
            recursive_for_loop
            cd ..
        fi
    done
}
recursive_for_loop
