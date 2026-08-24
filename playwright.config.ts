import { defineConfig, devices } from '@playwright/test';

const webPort = process.env.K10S_E2E_PORT ?? '18080';
const webURL = `http://127.0.0.1:${webPort}`;

// The foundation smoke builds the Trunk distribution and serves it through the
// standalone k10s server exactly as production does.
export default defineConfig({
  testDir: 'tests/browser',
  timeout: 60_000,
  expect: { timeout: 10_000 },
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI
    ? [['line'], ['html', { outputFolder: 'playwright-report', open: 'never' }]]
    : 'list',
  outputDir: 'test-results',
  use: {
    baseURL: webURL,
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
    {
      name: 'firefox',
      grep: /@compat/,
      use: { ...devices['Desktop Firefox'] },
    },
    {
      name: 'webkit',
      grep: /@compat/,
      use: { ...devices['Desktop Safari'] },
    },
  ],
  webServer: {
    command:
      // Browser foundation tests exercise the UI shell only, so they select
      // fake data explicitly; real-cluster launches default to Kube mode.
      `trunk build --release && cargo run --locked --release -p k10s-server-app --bin k10s-server -- --fake --token-file tests/browser/token.txt --listen 127.0.0.1:${webPort}`,
    url: webURL,
    reuseExistingServer: !process.env.CI,
    timeout: 900_000,
    env: {
      K10S_DIST_DIR: 'dist',
    },
  },
});
