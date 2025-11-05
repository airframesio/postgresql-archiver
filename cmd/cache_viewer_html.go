package cmd

import (
	"embed"
	"strings"
)

// Embed the web assets
//
//go:embed cmd/web/viewer.html cmd/web/styles.css cmd/web/script.js
var webAssets embed.FS

// Generate HTML with embedded assets
func generateCacheViewerHTML() string {
	// Read the HTML file
	htmlContent, err := webAssets.ReadFile("cmd/web/viewer.html")
	if err != nil {
		return fallbackHTML()
	}

	// Read the CSS file
	cssContent, err := webAssets.ReadFile("cmd/web/styles.css")
	if err != nil {
		return fallbackHTML()
	}

	// Read the JavaScript file
	jsContent, err := webAssets.ReadFile("cmd/web/script.js")
	if err != nil {
		return fallbackHTML()
	}

	// Convert byte slices to strings
	html := string(htmlContent)
	css := string(cssContent)
	js := string(jsContent)

	// Replace the stylesheet link with inline CSS
	html = strings.Replace(html, `    <link rel="stylesheet" href="styles.css">`,
		`    <style>`+"\n"+css+"\n"+`    </style>`, 1)

	// Replace the script src with inline script
	html = strings.Replace(html, `    <script src="script.js"></script>`,
		`    <script>`+"\n"+js+"\n"+`    </script>`, 1)

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
