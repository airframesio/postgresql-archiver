#!/bin/sh
set -e

echo "🔧 Minifying web assets..."

# Create minified directory if it doesn't exist
mkdir -p cmd/web

# Minify CSS
echo "📦 Minifying styles.css..."
npx csso-cli cmd/web/styles.css -o cmd/web/styles.min.css
echo "   Original: $(wc -c < cmd/web/styles.css) bytes"
echo "   Minified: $(wc -c < cmd/web/styles.min.css) bytes"

# Minify JavaScript
echo "📦 Minifying script.js..."
npx terser cmd/web/script.js -o cmd/web/script.min.js \
  --compress \
  --mangle \
  --comments false
echo "   Original: $(wc -c < cmd/web/script.js) bytes"
echo "   Minified: $(wc -c < cmd/web/script.min.js) bytes"

# Minify HTML (viewer.html)
echo "📦 Minifying viewer.html..."
npx html-minifier-terser cmd/web/viewer.html \
  --collapse-whitespace \
  --remove-comments \
  --remove-optional-tags \
  --remove-redundant-attributes \
  --remove-script-type-attributes \
  --remove-tag-whitespace \
  --use-short-doctype \
  --minify-css true \
  --minify-js true \
  -o cmd/web/viewer.min.html
echo "   Original: $(wc -c < cmd/web/viewer.html) bytes"
echo "   Minified: $(wc -c < cmd/web/viewer.min.html) bytes"

# Minify design system HTML
echo "📦 Minifying design system index.html..."
npx html-minifier-terser docs/design-system/index.html \
  --collapse-whitespace \
  --remove-comments \
  --minify-css true \
  --minify-js true \
  -o docs/design-system/index.min.html
echo "   Original: $(wc -c < docs/design-system/index.html) bytes"
echo "   Minified: $(wc -c < docs/design-system/index.min.html) bytes"

echo "✅ Minification complete!"
echo ""
echo "Summary:"
echo "--------"
total_original=$(($(wc -c < cmd/web/styles.css) + $(wc -c < cmd/web/script.js) + $(wc -c < cmd/web/viewer.html) + $(wc -c < docs/design-system/index.html)))
total_minified=$(($(wc -c < cmd/web/styles.min.css) + $(wc -c < cmd/web/script.min.js) + $(wc -c < cmd/web/viewer.min.html) + $(wc -c < docs/design-system/index.min.html)))
savings=$((total_original - total_minified))
percent=$((savings * 100 / total_original))
echo "Total original size: $total_original bytes"
echo "Total minified size: $total_minified bytes"
echo "Space saved: $savings bytes (${percent}%)"
