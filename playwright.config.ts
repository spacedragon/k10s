import { defineConfig, devices } from '@playwright/test';

const webPort = process.env.K10S_E2E_PORT ?? '8080';
const webURL = `http://127.0.0.1:${webPort}`;

// The foundation smoke builds the Trunk distribution and serves it through the
// standalone k10s server exactly as production does.
export default defineConfig({
  testDir: 'tests/browser',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? 'line' : 'list',
  use: { baseURL: webURL },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    command:
      // Browser foundation tests exercise the UI shell only, so they select
      // fake data explicitly; real-cluster launches default to Kube mode.
      'trunk build --release && cargo run --locked --release -p k10s-server-app -- --fake',
    url: webURL,
    reuseExistingServer: !process.env.CI,
    timeout: 900_000,
    env: {
      K10S_BIND_ADDR: `127.0.0.1:${webPort}`,
      K10S_ACCESS_TOKEN: 'foundation-secret',
      K10S_DIST_DIR: 'dist',
    },
  },
});
