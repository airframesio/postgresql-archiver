package cmd

import (
	"embed"
	"strings"
)

// Embed the web assets (minified for production)
//
//go:embed web/viewer.min.html web/styles.min.css web/script.min.js
var webAssets embed.FS

// Generate HTML with embedded assets
func generateCacheViewerHTML() string {
	// Read the minified HTML file
	htmlContent, err := webAssets.ReadFile("web/viewer.min.html")
	if err != nil {
		return fallbackHTML()
	}

	// Read the minified CSS file
	cssContent, err := webAssets.ReadFile("web/styles.min.css")
	if err != nil {
		return fallbackHTML()
	}

	// Read the minified JavaScript file
	jsContent, err := webAssets.ReadFile("web/script.min.js")
	if err != nil {
		return fallbackHTML()
	}

	// Convert byte slices to strings
	html := string(htmlContent)
	css := string(cssContent)
	js := string(jsContent)

	// Replace the stylesheet link with inline CSS (minified HTML has no spaces/indentation)
	html = strings.Replace(html, `<link rel="stylesheet"href="styles.css">`,
		`<style>`+css+`</style>`, 1)

	// Replace the script src with inline script (minified HTML has no spaces/indentation)
	html = strings.Replace(html, `<script src="script.js">`,
		`<script>`+js, 1)

	return html
}

// Package-level variable initialized on startup
var cacheViewerHTML string

// Initialize HTML on package load
func init() {
	cacheViewerHTML = generateCacheViewerHTML()
}

// Fallback HTML for when embed files are missing
func fallbackHTML() string {
	return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL Archiver - Cache Viewer</title>
    <style>
        body {
            font-family: system-ui, -apple-system, sans-serif;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            margin: 0;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        }
        .error-container {
            background: white;
            padding: 2rem;
            border-radius: 8px;
            text-align: center;
            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.1);
        }
        h1 {
            color: #333;
            margin-bottom: 0.5rem;
        }
        p {
            color: #666;
        }
    </style>
</head>
<body>
    <div class="error-container">
        <h1>Cache Viewer Unavailable</h1>
        <p>The embedded web assets could not be loaded.</p>
        <p>Please ensure the web/ directory files are properly embedded.</p>
    </div>
</body>
</html>`
}
