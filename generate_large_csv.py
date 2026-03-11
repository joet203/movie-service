import csv
import random
import os

ADJECTIVES = [
    "Dark", "Lost", "Silent", "Broken", "Last", "Crimson", "Eternal", "Savage",
    "Hidden", "Fallen", "Iron", "Golden", "Wicked", "Forgotten", "Burning",
    "Frozen", "Hollow", "Shattered", "Twisted", "Endless", "Midnight", "Sacred",
    "Bitter", "Reckless", "Deadly", "Ruthless", "Cursed", "Fading", "Desperate",
    "Violent", "Ancient", "Blinding", "Restless", "Distant", "Phantom", "Neon",
    "Electric", "Atomic", "Velvet", "Steel", "Glass", "Shadow", "Thunder",
    "Blazing", "Haunted", "Spectral", "Infernal", "Celestial", "Primal", "Feral",
]

NOUNS = [
    "Highway", "Kingdom", "Protocol", "Legacy", "Redemption", "Horizon",
    "Vendetta", "Requiem", "Conspiracy", "Reckoning", "Prophecy", "Dominion",
    "Abyss", "Frontier", "Paradox", "Uprising", "Vengeance", "Oblivion",
    "Nemesis", "Labyrinth", "Inferno", "Eclipse", "Phantom", "Exodus",
    "Syndicate", "Catalyst", "Vanguard", "Crucible", "Tempest", "Mirage",
    "Colossus", "Citadel", "Rampage", "Siege", "Verdict", "Witness",
    "Chronicle", "Fugitive", "Outlaw", "Patriot", "Prisoner", "Stranger",
    "Warrior", "Assassin", "Detective", "Survivor", "Guardian", "Wanderer",
    "Prophet", "Overlord",
]

PREFIXES = [
    "The", "Return of the", "Rise of the", "Fall of the", "Beyond the",
    "Into the", "Escape from the", "Night of the", "Day of the", "Edge of the",
    "", "", "", "",
]

GENRES = [
    "Action", "Comedy", "Drama", "Horror", "Romance", "Thriller",
    "Sci-Fi", "Adventure", "Crime", "Mystery", "Fantasy", "Animation",
]

NUM_ROWS = 10_000_000
OUTPUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "movies_large.csv")
MISSING_RATE = 0.05


def random_movie_name(rng):
    prefix = rng.choice(PREFIXES)
    adj = rng.choice(ADJECTIVES)
    noun = rng.choice(NOUNS)
    if prefix:
        return f"{prefix} {adj} {noun}"
    return f"{adj} {noun}"


def random_genres(rng):
    count = rng.choices([1, 2, 3], weights=[50, 35, 15])[0]
    picked = rng.sample(GENRES, count)
    return ", ".join(picked)


def main():
    rng = random.Random(42)
    print(f"Generating {NUM_ROWS:,} rows to {OUTPUT}")

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["movie_name", "year", "genres", "rating"])

        for i in range(1, NUM_ROWS + 1):
            name = random_movie_name(rng)
            year = "" if rng.random() < MISSING_RATE else str(rng.randint(1920, 2025))
            genres = random_genres(rng)
            rating = "" if rng.random() < MISSING_RATE else f"{rng.uniform(1.0, 10.0):.1f}"
            writer.writerow([name, year, genres, rating])

            if i % 500_000 == 0:
                print(f"  {i:,} / {NUM_ROWS:,} rows written")

    size_mb = os.path.getsize(OUTPUT) / (1024 * 1024)
    print(f"Done. File size: {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
