#!/bin/sh

set -eu

run_root=${1:-}
case "$run_root" in
    /*) ;;
    *) echo "cleanup root must be absolute" >&2; exit 2 ;;
esac

collect_tree() {
    # Capture before starting the parser. Otherwise the parser's own command
    # line contains run_root and can appear in the `ps` output it is reading.
    process_table=$(ps -ef)
    collected=$(printf '%s\n' "$process_table" | awk -v root="$run_root" -v self="$$" '
        NR > 1 && $2 != self && $3 != self && index($0, root) != 0 { print $2 }
    ')

    # A fixture daemon may be blocked in a generic `sleep` whose argv no
    # longer contains the test root. Freeze descendants while their parent
    # relationships are still observable.
    descendants=$collected
    for _depth in 1 2 3 4; do
        process_table=$(ps -ef)
        children=$(printf '%s\n' "$process_table" | awk -v parents="$descendants" '
            BEGIN {
                count = split(parents, values)
                for (item = 1; item <= count; item++) {
                    parent[values[item]] = 1
                }
            }
            NR > 1 && ($3 in parent) { print $2 }
        ')
        test -n "$children" || break
        for pid in $children; do
            test "$pid" != "$$" || continue
            case " $collected " in
                *" $pid "*) ;;
                *) collected="$collected $pid" ;;
            esac
        done
        descendants=$children
    done

    printf '%s\n' "$collected"
}

for _round in 1 2 3; do
    current=$(collect_tree)
    for pid in $current; do
        kill "$pid" 2>/dev/null || true
    done
    /usr/bin/sleep 0.2
done

# Kill the deepest descendants first. This prevents a final blocked `sleep`
# from being orphaned when its fixture shell is force-removed.
current=$(collect_tree)
reverse=$(printf '%s\n' $current | awk '{ values[NR]=$0 } END {
    for (item=NR; item >= 1; item--) print values[item]
}')
for pid in $reverse; do
    kill -9 "$pid" 2>/dev/null || true
done
/usr/bin/sleep 0.1

attempts=20
survivors=$(collect_tree)
while test -n "$survivors" && test "$attempts" -gt 0; do
    /usr/bin/sleep 0.05
    attempts=$((attempts - 1))
    survivors=$(collect_tree)
done
test -z "$survivors" || {
    echo "test processes survived cleanup:$survivors" >&2
    exit 1
}
