import { expect, test } from '@playwright/test';
import { connect } from './helpers';

// Captures the redesigned Deployment List + Detail view for #193 so the PR
// can be compared directly with docs/designs/10-list-detail-redesign.html.
// The reference shows a wide two-pane layout: a compact list on the left and
// the Overview detail (PODS/ROLLOUT tables, Template/Labels/Identity, chips)
// on the right. We render that at the reference's wide geometry (1000x700
// egui points) and also at the 640x700 one-column breakpoint.

async function openDeploymentOverview(page: import('@playwright/test').Page) {
  await page.getByRole('button', { name: 'Deployments', exact: true }).dispatchEvent('click');
  await expect(page.getByRole('table', { name: 'Deployments' })).toBeVisible();
  // Select a healthy deployment that has pods and rollout history.
  await page
    .getByRole('button', { name: 'web-frontend', exact: true })
    .dispatchEvent('click');
  await expect(page.getByRole('heading', { name: 'web-frontend details' })).toBeVisible();
  // The Overview tab is the default; confirm the detail chrome is up. The
  // Overview body itself (PODS/TEMPLATE tables, chips) is canvas-rendered and
  // painted on the next frames, so settle before capturing.
  await expect(page.getByRole('button', { name: 'Overview', exact: true })).toBeVisible();
  await page.waitForTimeout(400);
}

test('captures issue 193 redesigned list + detail at the reference wide geometry', async ({
  page,
}) => {
  await page.setViewportSize({ width: 1000, height: 700 });
  await connect(page);
  await page.locator('#k10s-canvas').click({ position: { x: 500, y: 350 } });
  await openDeploymentOverview(page);
  // Let the canvas finish its responsive relayout before Playwright compares
  // consecutive frames. The detail footer changes the available body height,
  // and capturing during that transition leaves the filter row one frame
  // behind the rest of the window.
  await page.waitForTimeout(1_000);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-193/after-list-detail-1000x700.png',
  });
  await expect(page).toHaveScreenshot('list-detail-1000x700.png', {
    animations: 'disabled',
    fullPage: true,
  });
});

test('captures issue 193 one-column overview breakpoint at 640x700', async ({ page }) => {
  await page.setViewportSize({ width: 640, height: 700 });
  await connect(page);
  await page.locator('#k10s-canvas').click({ position: { x: 320, y: 350 } });
  await openDeploymentOverview(page);
  await page.waitForTimeout(250);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-193/after-list-detail-640x700.png',
  });
  await expect(page).toHaveScreenshot('list-detail-640x700.png', {
    animations: 'disabled',
    fullPage: true,
  });
});
