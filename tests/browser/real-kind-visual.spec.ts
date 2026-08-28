import { expect, test } from '@playwright/test';
import { connect } from './helpers';

test('captures the read-only kind-bunyip visual baseline', async ({ page }) => {
  test.skip(process.env.K10S_REAL_KIND !== '1', 'opt-in real kind visual capture');

  await page.setViewportSize({ width: 1280, height: 800 });
  await connect(page);
  await expect(page.getByText('kind-bunyip', { exact: true })).toBeVisible();

  await page.getByRole('button', { name: 'Pods', exact: true }).dispatchEvent('click');
  const pods = page.getByRole('table', { name: 'Pods' });
  await expect(pods).toContainText('broken-');
  await expect(pods).toContainText('Pending');
  await expect(pods).toContainText('Running');
  await expect(pods.getByRole('row')).toHaveCount(19);
  await pods.getByRole('button', { name: /^broken-/ }).dispatchEvent('click');
  await expect(page.getByRole('heading', { name: /broken-.* details/ })).toBeVisible();
  await page
    .getByRole('tablist')
    .getByRole('button', { name: 'Events', exact: true })
    .dispatchEvent('click');
  await page.waitForTimeout(1_000);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-170/after-real-kind-pods-1280x800.png',
  });

  await page.getByRole('button', { name: 'Jobs', exact: true }).dispatchEvent('click');
  const jobs = page.getByRole('table', { name: 'Jobs' });
  await expect(jobs).toContainText('hello');
  await expect(jobs).toContainText('Complete');
  await page.waitForTimeout(500);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-170/after-real-kind-job-1280x800.png',
  });

  await page.getByRole('button', { name: 'StatefulSets', exact: true }).dispatchEvent('click');
  const statefulSets = page.getByRole('table', { name: 'StatefulSets' });
  await expect(statefulSets).toContainText('web');
  await expect(statefulSets).toContainText('2/2');
  await page.waitForTimeout(500);
  await page.screenshot({
    animations: 'disabled',
    fullPage: true,
    path: 'docs/screenshots/issue-170/after-real-kind-statefulset-1280x800.png',
  });
});
