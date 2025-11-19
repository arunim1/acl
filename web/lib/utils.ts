import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(num: number, decimals = 2): string {
  return num.toFixed(decimals);
}

export function formatLargeNumber(num: number): string {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  }
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
}

export function formatTimeControl(tc: string): string {
  const [base, increment] = tc.split('+');
  const baseMinutes = parseInt(base) / 60;
  return `${baseMinutes}+${increment}`;
}

export function downloadFile(content: string, filename: string, type = 'text/csv') {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

export function getTimeControlCategory(tc: string): string {
  const [base] = tc.split('+').map(Number);

  if (base < 180) return 'Bullet';
  if (base < 600) return 'Blitz';
  if (base < 1500) return 'Rapid';
  return 'Classical';
}
