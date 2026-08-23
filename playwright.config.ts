import { defineConfig, devices } from '@playwright/test';

// The foundation smoke builds the Trunk distribution and serves it through the
// standalone k10s server exactly as production does.
export default defineConfig({
  testDir: 'tests/browser',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'line' : 'list',
  use: { baseURL: 'http://127.0.0.1:8080' },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command:
      // Browser foundation tests exercise the UI shell only, so they select
      // fake data explicitly; real-cluster launches default to Kube mode.
      'trunk build --release && cargo run --locked --release -p k10s-server-app -- --fake',
    url: 'http://127.0.0.1:8080',
    reuseExistingServer: !process.env.CI,
    timeout: 900_000,
    env: {
      K10S_BIND_ADDR: '127.0.0.1:8080',
      K10S_ACCESS_TOKEN: 'foundation-secret',
      K10S_DIST_DIR: 'dist',
    },
  },
});
