/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: { extend: { colors: { ink: { 900: "#0f172a", 700: "#334155", 500: "#64748b", 300: "#cbd5e1", 100: "#f1f5f9" }, brand: { 600: "#1d4ed8", 700: "#1e40af", 50: "#eff6ff" } },
    fontFamily: { sans: ["Inter", "system-ui", "Segoe UI", "Roboto", "sans-serif"], mono: ["ui-monospace", "SFMono-Regular", "Consolas", "monospace"] } } },
  plugins: [],
};
