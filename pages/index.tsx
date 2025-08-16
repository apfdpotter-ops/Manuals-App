import { useEffect, useState } from "react";

export default function Home() {
  const [manuals, setManuals] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function fetchManuals() {
      try {
        // inside a React component (e.g., useEffect or an event handler)
const res = await fetch('/api/data?ts=' + Date.now(), { cache: 'no-store' });
const data = await res.json();
        setManuals(data);
      } catch (err) {
        console.error("Failed to fetch manuals", err);
      } finally {
        setLoading(false);
      }
    }
    fetchManuals();
  }, []);

  return (
    <main style={{ padding: "2rem", fontFamily: "sans-serif" }}>
      <h1>Manuals App</h1>
      <input
        placeholder="Search manuals..."
        style={{
          padding: "0.5rem",
          marginBottom: "1rem",
          width: "100%",
          maxWidth: "400px",
        }}
      />
      {loading ? (
        <p>Loading...</p>
      ) : (
        manuals.map((m) => (
          <div key={m.id} style={{ margin: "1rem 0" }}>
            <h3>{m.title}</h3>
            <p>{m.brand} • {m.model} • {m.year}</p>
            <a href={m.url} target="_blank" rel="noopener noreferrer">
              Open
            </a>
          </div>
        ))
      )}
    </main>
  );
}
