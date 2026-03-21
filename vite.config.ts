import arial from "@capsizecss/metrics/arial";
import inter from "@capsizecss/metrics/inter";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import { capsizeRadixPlugin } from "vite-plugin-capsize-radix";
import viteTsConfigPaths from "vite-tsconfig-paths";

const config = defineConfig({
	plugins: [
		viteTsConfigPaths({
			projects: ["./tsconfig.json"],
		}),
		capsizeRadixPlugin({
			outputPath: "./public/typography.css",
			defaultFontStack: [inter, arial],
		}),
		tanstackStart(),
		viteReact(),
	],
	server: {
		port: 5173,
	},
});

export default config;
