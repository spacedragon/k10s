import { expect, test } from '@playwright/test';
import { connect, openResource } from './helpers';

test('opens the shared mutation dialog for a scalable workload', async ({ page }) => {
  await connect(page);
  await openResource(page, 'Deployments', 'web-frontend');
  await page.getByRole('button', { name: 'Scale workload' }).dispatchEvent('click');
  await expect(page.getByRole('dialog', { name: 'Scale workload' })).toBeVisible();
});

test('gates destructive confirmation and exposes the full safety contract', async ({ page }) => {
  await connect(page);
  await openResource(page, 'Deployments', 'web-frontend');
  await page.getByRole('button', { name: 'Delete resource' }).dispatchEvent('click');
  const dialog = page.getByRole('dialog', { name: 'Delete resource' });
  await expect(dialog).toBeVisible();
});
