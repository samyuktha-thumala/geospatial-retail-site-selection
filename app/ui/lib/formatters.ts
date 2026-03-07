export const formatCurrency = (amount: number) =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(amount);

export const formatNumber = (num: number) =>
  new Intl.NumberFormat("en-US").format(num);

export const formatPercent = (num: number) =>
  `${num >= 0 ? "+" : ""}${num.toFixed(1)}%`;
