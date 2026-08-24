import { expect, test } from '@playwright/test';
import { connect, openResource } from './helpers';

test('opens the shared mutation dialog for a scalable workload', async ({ page }) => {
  await connect(page);
  await openResource(page, 'Deployments', 'web-frontend');
  await page.getByRole('button', { name: 'Scale workload' }).click();
  await expect(page.getByRole('dialog', { name: 'Scale workload' })).toBeVisible();
});
