import { expect, test } from '@playwright/test';
import { connect, openResource } from './helpers';

const phase = process.env.K10S_SCREENSHOT_PHASE ?? 'after';

test('captures issue 169 fixed-viewport investigation ergonomics', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await connect(page);

  await page.locator('#k10s-canvas').click({ position: { x: 640, y: 360 } });
  await page.keyboard.press('Control+k');
  await page.waitForTimeout(300);
  await page.screenshot({
    animations: 'disabled',
    path: `docs/screenshots/issue-169/${phase}-palette-1280x800.png`,
  });

  await page.keyboard.press('Escape');
  await page.setViewportSize({ width: 1440, height: 1000 });
  await openResource(page, 'Pods', 'db-postgres-0');
  await page.getByRole('button', { name: 'Logs', exact: true }).dispatchEvent('click');
  await expect(page.getByRole('button', { name: 'Connect logs' })).toBeVisible();
  await page.waitForTimeout(300);
  await page.screenshot({
    animations: 'disabled',
    path: `docs/screenshots/issue-169/${phase}-detail-logs-1440x1000.png`,
  });
});
