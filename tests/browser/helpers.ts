import { expect, Page } from '@playwright/test';

export async function connect(page: Page): Promise<void> {
  await page.goto('/');
  await page.getByLabel('Access token').fill('foundation-secret');
  await page.getByRole('button', { name: 'Connect' }).click();
  await expect(page.getByRole('heading', { name: 'k10s Workspace' })).toBeVisible();
  await expect(page.getByRole('status')).toHaveText('Connected');
}

export async function openResource(
  page: Page,
  kind: string,
  name: string,
): Promise<void> {
  await page.getByRole('button', { name: kind, exact: true }).click();
  await expect(page.getByRole('table', { name: kind })).toBeVisible();
  await page.getByRole('button', { name, exact: true }).click();
  await expect(page.getByRole('heading', { name: `${name} details` })).toBeVisible();
}
