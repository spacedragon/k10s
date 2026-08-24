import { expect, test } from '@playwright/test';
import { connect } from './helpers';

test('reconnects and completes a full resync after transport loss @compat', async ({ page }) => {
  await connect(page);
  await page.getByRole('button', { name: 'Reconnect control connection' }).click();
  await expect(page.getByRole('status')).toHaveText('Reconnecting and resyncing');
  await expect(page.getByRole('heading', { name: 'k10s Workspace' })).toBeVisible({
    timeout: 30_000,
  });
  await expect(page.getByRole('status')).toHaveText('Connected');
  await expect(page.getByText('dev-local')).toBeVisible();
});
