#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")" && pwd)
SDK=${ANDROID_SDK_ROOT:-${ANDROID_HOME:-$HOME/android-sdk}}
if [ -z "${BUILD_TOOLS:-}" ]; then
    BUILD_TOOLS=$(find "$SDK/build-tools" -mindepth 1 -maxdepth 1 -type d 2>/dev/null \
        | sort -V | tail -1)
fi
PLATFORM_JAR=${PLATFORM_JAR:-$SDK/platforms/android-34/android.jar}
OUT="$ROOT/build"
APK="$OUT/yi-debug.apk"

for tool in aapt2 d8 zipalign apksigner; do
    if [ ! -x "$BUILD_TOOLS/$tool" ]; then
        echo "missing $BUILD_TOOLS/$tool" >&2
        echo "set ANDROID_SDK_ROOT or install build-tools;34.0.0" >&2
        exit 1
    fi
done
if [ ! -f "$PLATFORM_JAR" ]; then
    echo "missing $PLATFORM_JAR" >&2
    echo "install platforms;android-34" >&2
    exit 1
fi

rm -rf "$OUT/classes" "$OUT/dex" "$OUT/gen" "$OUT/compiled.zip" "$OUT/base.apk" "$OUT/unsigned.apk" "$OUT/aligned.apk" "$APK"
mkdir -p "$OUT/classes" "$OUT/dex" "$OUT/gen"

echo "==> resources"
"$BUILD_TOOLS/aapt2" compile --dir "$ROOT/res" -o "$OUT/compiled.zip"
"$BUILD_TOOLS/aapt2" link \
    -o "$OUT/base.apk" \
    -I "$PLATFORM_JAR" \
    --manifest "$ROOT/AndroidManifest.xml" \
    --min-sdk-version 24 \
    --target-sdk-version 34 \
    --auto-add-overlay \
    --debug-mode \
    --java "$OUT/gen" \
    "$OUT/compiled.zip"

echo "==> java"
find "$ROOT/src" "$OUT/gen" -name '*.java' -print > "$OUT/sources.txt"
javac --release 8 -encoding UTF-8 -Xlint:-options -classpath "$PLATFORM_JAR" \
    -d "$OUT/classes" @"$OUT/sources.txt"

echo "==> dex"
find "$OUT/classes" -name '*.class' -print > "$OUT/classes.txt"
"$BUILD_TOOLS/d8" --lib "$PLATFORM_JAR" --min-api 24 \
    --output "$OUT/dex" @"$OUT/classes.txt"

echo "==> package"
cp "$OUT/base.apk" "$OUT/unsigned.apk"
(cd "$OUT" && zip -q -j unsigned.apk dex/classes.dex)
"$BUILD_TOOLS/zipalign" -f 4 "$OUT/unsigned.apk" "$OUT/aligned.apk"

if [ ! -f "$OUT/debug.keystore" ]; then
    echo "==> debug key"
    keytool -genkeypair -v \
        -keystore "$OUT/debug.keystore" \
        -alias androiddebugkey \
        -storepass android \
        -keypass android \
        -keyalg RSA \
        -keysize 2048 \
        -validity 10000 \
        -dname "CN=Android Debug,O=Android,C=US" >/dev/null
fi

echo "==> sign"
"$BUILD_TOOLS/apksigner" sign \
    --ks "$OUT/debug.keystore" \
    --ks-pass pass:android \
    --key-pass pass:android \
    --out "$APK" \
    "$OUT/aligned.apk"
"$BUILD_TOOLS/apksigner" verify "$APK"

echo "APK: $APK"

if [ "${1:-}" = "install" ]; then
    adb install -r "$APK"
    adb shell am start -n com.gresces.yi/.MainActivity
fi
