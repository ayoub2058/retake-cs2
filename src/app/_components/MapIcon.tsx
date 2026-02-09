"use client";

import { useEffect, useMemo, useState } from "react";

type MapIconProps = {
  mapName: string | null;
  sizeClassName?: string;
};

const toMapSlug = (value: string | null) => {
  if (!value) {
    return null;
  }
  const slug = value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .replace(/__+/g, "_");
  return slug || null;
};

const getCandidates = (mapName: string | null) => {
  const slug = toMapSlug(mapName);
  if (!slug) {
    return [] as string[];
  }
  const titleCase = slug.replace(/^./, (char) => char.toUpperCase());
  return [
    `/map_icon/Map_icon_${slug}.webp`,
    `/map_icon/Map_icon_${slug}.png`,
    `/map_icon/${titleCase}.webp`,
    `/map_icon/${titleCase}.png`,
  ];
};

export const MapIcon = ({ mapName, sizeClassName }: MapIconProps) => {
  const candidates = useMemo(() => getCandidates(mapName), [mapName]);
  const [index, setIndex] = useState(0);
  const [src, setSrc] = useState<string | null>(null);

  useEffect(() => {
    setIndex(0);
    setSrc(candidates[0] ?? null);
  }, [candidates]);

  if (!src) {
    return null;
  }
  return (
    <img
      src={src}
      alt={mapName ? `${mapName} icon` : "Map icon"}
      className={
        sizeClassName ??
        "h-12 w-12 rounded-xl border border-white/10 bg-white/5 object-contain"
      }
      onError={() => {
        setIndex((current) => {
          const next = current + 1;
          const nextSrc = candidates[next] ?? null;
          if (nextSrc) {
            setSrc(nextSrc);
          }
          return next;
        });
      }}
    />
  );
};
