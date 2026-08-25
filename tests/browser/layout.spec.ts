import { expect, test } from '@playwright/test';
import { connect } from './helpers';

test('remains usable at the supported 640x420 minimum viewport', async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 420 });
  await connect(page);
  await page.getByRole('button', { name: 'Deployments', exact: true }).dispatchEvent('click');
  await expect(page.getByRole('table', { name: 'Deployments' })).toBeVisible();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - innerWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  const renderedPixels = await page.locator('#k10s-canvas').evaluate((canvas: HTMLCanvasElement) => {
    const context = canvas.getContext('2d');
    if (context) {
      const pixels = context.getImageData(0, 0, canvas.width, canvas.height).data;
      return pixels.some((value, index) => index % 4 !== 3 && value !== 0);
    }
    // WebGL canvases cannot be read through a 2D context, but a real eframe
    // surface is sized by its resize observer; the old DOM-only host had none.
    return canvas.width >= 640 && canvas.height >= 420;
  });
  expect(renderedPixels).toBeTruthy();
  await expect(page).toHaveScreenshot('compact-workspace.png', {
    animations: 'disabled',
    fullPage: true,
  });
});
