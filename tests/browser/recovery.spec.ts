import { expect, test, WebSocketRoute } from '@playwright/test';
import { connect } from './helpers';

test('reconnects and completes a full resync after a real transport loss @compat', async ({ page }) => {
  let controlSocket: WebSocketRoute | undefined;
  await page.routeWebSocket('**/api/v1/control', socket => {
    controlSocket = socket;
    socket.connectToServer();
  });
  await connect(page);
  // Close the live browser WebSocket from below the application. This reaches
  // the production close callback; no reconnect UI or internal state seam is used.
  expect(controlSocket).toBeDefined();
  await controlSocket!.close({ code: 1012, reason: 'browser transport loss gate' });
  await expect(page.getByRole('status')).toHaveText('Reconnecting and resyncing');
  await expect(page.getByRole('heading', { name: 'k10s Workspace' })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole('status')).toHaveText('Connected');
  await expect(page.getByText('dev-local')).toBeVisible();
});
