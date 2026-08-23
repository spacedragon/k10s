import { expect, test } from '@playwright/test';

test('authenticates against the server-hosted app and shows fake contexts', async ({ page }) => {
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Connect to k10s' })).toBeVisible();
  await page.getByLabel('Access token').fill('wrong-token');
  await page.getByRole('button', { name: 'Connect' }).click();
  await expect(page.getByText('Authentication failed. Try again.')).toBeVisible();

  await page.getByLabel('Access token').fill('foundation-secret');
  await page.getByRole('button', { name: 'Connect' }).click();

  await expect(page.getByRole('heading', { name: 'Kubernetes contexts' })).toBeVisible();
  await expect(page.getByText('dev-local')).toBeVisible();
  await expect(page.getByText('prod-readonly')).toBeVisible();
  await expect(page.getByLabel('Access token')).toHaveCount(0);
  await expect(page).not.toHaveURL(/foundation-secret/);
});
