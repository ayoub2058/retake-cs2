# Retake (CS2 Match Analytics)

Retake is a CS2 match analytics dashboard that collects demos, parses rounds, and surfaces player performance insights.

## Tech Stack

- Next.js 14 (App Router)
- Supabase (Postgres + Auth)
- Tailwind CSS
- Shadcn UI

## Features

- Player search
- Match history
- Round analysis and timeline

## Setup

1) Clone the repo
2) Install dependencies

```bash
npm install
```

3) Configure environment variables

```bash
cp .env.example .env.local
```

Fill in the values in `.env.local` with your own keys.

4) Run the dev server

```bash
npm run dev
```

Open http://localhost:3000 to view the app.

## Notes

- Demo files downloaded to `downloads/` are generated locally and are ignored by git.
