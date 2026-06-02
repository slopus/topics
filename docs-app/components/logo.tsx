export function LogoMark({ size = 26 }: { size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 32 32"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
      className="topics-logo-mark"
    >
      <defs>
        <linearGradient id="topics-g" x1="2" y1="3" x2="30" y2="29" gradientUnits="userSpaceOnUse">
          <stop stopColor="var(--topics-accent-2)" />
          <stop offset="1" stopColor="var(--topics-accent)" />
        </linearGradient>
      </defs>
      <rect x="1.25" y="1.25" width="29.5" height="29.5" rx="8" stroke="url(#topics-g)" strokeWidth="1.5" />
      {/* append-only log: three records growing toward head, with a flowing cursor dot */}
      <rect x="7" y="9" width="13" height="2.6" rx="1.3" fill="url(#topics-g)" opacity="0.55" />
      <rect x="7" y="14.7" width="18" height="2.6" rx="1.3" fill="url(#topics-g)" opacity="0.8" />
      <rect x="7" y="20.4" width="9.5" height="2.6" rx="1.3" fill="url(#topics-g)" />
      <circle cx="22.5" cy="21.7" r="2.4" fill="var(--topics-accent)" />
    </svg>
  )
}

export function Logo({ small = false }: { small?: boolean }) {
  return (
    <span className={`topics-logo${small ? ' topics-logo--sm' : ''}`}>
      <LogoMark size={small ? 22 : 26} />
      <span className="topics-wordmark">topics</span>
    </span>
  )
}
