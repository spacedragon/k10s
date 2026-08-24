import { expect, test } from '@playwright/test';
import { connect, openResource } from './helpers';

test('opens bounded Logs and interactive Exec sockets for a selected Pod', async ({ page }) => {
  await connect(page);
  await openResource(page, 'Pods', 'api-server-5cc4d-qw8rt');

  await page.getByRole('button', { name: 'Connect logs' }).click();
  await expect(page.getByText('Logs: Streaming')).toBeVisible();
  await expect(page.locator('pre').filter({ hasText: 'api-server-5cc4d-qw8rt' }).first()).toBeVisible();

  await page.getByRole('button', { name: 'Connect shell' }).click();
  await expect(page.getByText('Exec: Attached')).toBeVisible();
});
