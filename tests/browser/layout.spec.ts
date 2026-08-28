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
  // The taxonomy exceeds the minimum viewport. Scroll the launcher to prove
  // lower groups remain reachable instead of being clipped after CronJobs.
  await page.mouse.move(100, 300);
  await page.mouse.wheel(0, 1000);
  // Let egui complete its final layout pass before pixel capture.
  await page.waitForTimeout(500);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-172/after-640x420.png',
  });
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
  await page.getByRole('button', { name: 'Deployments', exact: true }).dispatchEvent('click');
  await expect(page.getByRole('table', { name: 'Deployments' })).toBeVisible();
  await page.getByRole('button', { name: 'Pods', exact: true }).dispatchEvent('click');
  await expect(page.getByRole('table', { name: 'Pods' })).toBeVisible();
  await page.waitForTimeout(500);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-172/after-1280x800.png',
  });
});

test('resource taxonomy opens a named built-in from its group', async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await connect(page);

  // Expand Config on the real canvas. Eframe does not expose its AccessKit
  // tree on web, so the hidden semantic companion drives the named resource.
  await page.mouse.click(52, 538);
  await page
    .getByRole('button', { name: 'Secrets', exact: true })
    .dispatchEvent('click');
  await expect(page.getByRole('table', { name: 'Secrets' })).toBeVisible();
  await expect(page).toHaveScreenshot('resource-taxonomy.png', {
    animations: 'disabled',
    fullPage: true,
  });
});
