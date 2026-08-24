import { expect, test } from '@playwright/test';
import { connect } from './helpers';

test('remains usable at the supported 640x420 minimum viewport', async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 420 });
  await connect(page);
  await page.getByRole('button', { name: 'Deployments', exact: true }).click();
  await expect(page.getByRole('table', { name: 'Deployments' })).toBeVisible();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - innerWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  await expect(page).toHaveScreenshot('compact-workspace.png', {
    animations: 'disabled',
    fullPage: true,
  });
});
