import { expect, test } from '@playwright/test';
import { connect } from './helpers';

const supportedViewports = [
  { width: 640, height: 420 },
  { width: 1280, height: 800 },
  { width: 1440, height: 900 },
];

async function expectCanvasFitsViewport(page: Parameters<typeof connect>[0]) {
  const box = await page.locator('#k10s-canvas').boundingBox();
  expect(box).not.toBeNull();
  expect(box?.x).toBe(0);
  expect(box?.y).toBe(0);
  expect(box?.width).toBe(await page.evaluate(() => innerWidth));
  expect(box?.height).toBe(await page.evaluate(() => innerHeight));
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - innerWidth);
  expect(overflow).toBeLessThanOrEqual(0);
}

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

for (const viewport of supportedViewports) {
  test(`fits the shell at ${viewport.width}x${viewport.height}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await connect(page);
    await expectCanvasFitsViewport(page);
  });
}

test('matches the fixed 1280x800 shell composition', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await connect(page);
  await expectCanvasFitsViewport(page);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-152-shell-1280x800.png',
  });
});
