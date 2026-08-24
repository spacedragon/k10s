import { expect, test } from '@playwright/test';
import { connect, openResource } from './helpers';

test('selects a context resource list and backend-resolved detail', async ({ page }) => {
  await connect(page);
  await expect(page.getByText('dev-local')).toBeVisible();
  await openResource(page, 'Deployments', 'web-frontend');
  await expect(page.getByText('Status: 20/20 ready')).toBeVisible();
  await expect(page.getByRole('tablist')).toBeVisible();
  await expect(page.getByText('Kind: Deployment')).toBeVisible();
});
