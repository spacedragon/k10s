import { expect, test } from '@playwright/test';

test('the visible egui gate accepts and consumes the access token', async ({ page }) => {
  await page.goto('/');
  const canvas = page.locator('#k10s-canvas');
  await expect(canvas).toBeVisible();
  const bounds = await canvas.boundingBox();
  expect(bounds).not.toBeNull();

  // The fixed gate is the real egui renderer: focus its password field and
  // activate its button without using the semantic companion.
  await page.mouse.click(bounds!.x + 110, bounds!.y + 66);
  await page.keyboard.type('foundation-secret');
  await page.mouse.click(bounds!.x + 38, bounds!.y + 94);

  await expect(page.getByRole('heading', { name: 'k10s Workspace' })).toBeVisible();
  await expect(page.getByRole('status')).toHaveText('Connected');
  await expect(page.getByLabel('Access token')).toHaveCount(0);
});

test('authenticates against the server-hosted app and shows fake contexts @compat', async ({ page }) => {
  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Connect to k10s' })).toBeVisible();
  await page.getByLabel('Access token').fill('wrong-token');
  await page.getByRole('button', { name: 'Connect' }).dispatchEvent('click');
  await expect(page.getByText('Authentication failed. Try again.')).toBeVisible();

  await page.getByLabel('Access token').fill('foundation-secret');
  await page.getByRole('button', { name: 'Connect' }).dispatchEvent('click');

  await expect(page.getByRole('heading', { name: 'k10s Workspace' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Kubernetes contexts' })).toBeVisible();
  await expect(page.getByText('dev-local')).toBeVisible();
  await expect(page.getByText('prod-readonly')).toBeVisible();
  await expect(page.getByLabel('Access token')).toHaveCount(0);
  await expect(page).not.toHaveURL(/foundation-secret/);
  expect(await page.evaluate(() => JSON.stringify(localStorage))).not.toContain('foundation-secret');
});

test('a refreshed tab returns to a blank token gate', async ({ page }) => {
  await page.goto('/');
  await page.getByLabel('Access token').fill('foundation-secret');
  await page.getByRole('button', { name: 'Connect' }).dispatchEvent('click');
  await expect(page.getByRole('heading', { name: 'k10s Workspace' })).toBeVisible();

  await page.reload();
  await expect(page.getByRole('heading', { name: 'Connect to k10s' })).toBeVisible();
  await expect(page.getByLabel('Access token')).toHaveValue('');
});
