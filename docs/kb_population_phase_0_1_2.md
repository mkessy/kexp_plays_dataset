### Revised Population Plan & Relationship Ingestion Specification

This plan details the logic for populating the core relationships in our Knowledge Base schema, mapping from both the KEXP data and the MusicBrainz dump.

---

### **Updated: `kb_population_phase_0_1_2.md`**

This document outlines the phased approach to populating the KEXP Knowledge Base (KB), with detailed specifications for entity and relationship ingestion.

#### **Phase 0 & 1: Schema Creation & Foundation Entities (✓ Complete)**

This work is complete. The KB schema exists, and foundational tables (`kb_Genre`, `kb_Location`, `kb_Role`, `kb_Instrument`) have been populated from the MusicBrainz data dump.

---

#### **Phase 2: Core Entity & Relationship Population (Immediate Priority)**

This phase populates the remaining core entities and establishes the primary relationships from our structured KEXP and MusicBrainz data.

**1. Core Entity Population Logic (Refined)**

- **`kb_Song` (as a Recording):**

  - **Source**: `dim_tracks`.
  - **Logic**: Each row in `dim_tracks` represents a unique recording played on KEXP. We will create a `kb_Song` entity for each, using `dim_tracks.mb_recording_id` as the primary key for linking with MusicBrainz data. The `track_id_internal` from KEXP will be stored in the `bridge_kb_song_to_kexp` table.

- **`kb_Artist` & `kb_Person`:**

  - **Source**: `dim_artists_master` and `mb_artists_raw`.
  - **Logic**: We will iterate through `dim_artists_master`. For each artist with an `mb_id`, we will query `mb_artists_raw` to get its `type` (e.g., 'Person', 'Group').
    - If 'Person', we create a canonical entry in `kb_Person` and a corresponding entry in `kb_Artist` of type 'PERSON', linking the two.
    - If 'Group', we create an entry in `kb_Artist` of type 'GROUP'.
    - Artists without an `mb_id` will still be added to `kb_Artist` with a KEXP-generated UUID to ensure all played music is represented.

- **`kb_Album` (as a Release Group) & `kb_Release`:**
  - **Source**: `dim_releases_master`.
  - **Logic**: The `mb_release_group_id` will be used to create or link to a conceptual `kb_Album` entity. Each unique row in `dim_releases_master`, identified by `mb_release_id`, will create a `kb_Release` entity, which is then linked to its parent `kb_Album`.

**2. Relationship Population Specification**

This section details the logic for populating the `rel_*` tables. We will primarily query the `mb_relations_enhanced` view.

- **`rel_Artist_Member_Of_Artist` (Band Memberships)**

  - **Source**: `mb_relations_enhanced`
  - **Filter**: `WHERE relation_type = 'member of band' AND target_type = 'artist'`
  - **Logic**: This relationship has a `direction`.
    - If `direction = 'backward'`, the main artist of the row (`artist_mb_id`) is the **Group**, and the target artist (`target_entity_id`, from `artist_data.id`) is the **Member**.
    - If `direction = 'forward'`, the logic is reversed.
  - **Mapping**:
    - `kb_group_artist_id`: Find `kb_Artist.kb_id` where `kb_Artist.mb_artist_id` matches the group's MBID.
    - `kb_member_artist_id`: Find `kb_Artist.kb_id` where `kb_Artist.mb_artist_id` matches the member's MBID.
    - `start_date` / `end_date`: Directly map from the source columns.
  - **Example**: For AFI, the `artist_mb_id` is `1c391...`. A row with `relation_type = 'member of band'` and `direction = 'backward'` has a `target_entity_id` of `0d7b...` (Davey Havok). We link the `kb_id` for AFI to the `kb_id` for Davey Havok.

- **`rel_Artist_Plays_Instrument` (Instrument on Recording)**

  - **Source**: `mb_relations_enhanced`
  - **Filter**: `WHERE relation_type = 'instrument' AND target_type = 'recording'`
  - **Logic**: This relationship connects a performer to a specific instrument on a specific recording.
  - **Mapping**:
    - `kb_artist_id`: Map from `artist_mb_id`.
    - `kb_recording_id`: This is the `kb_song_id` in our schema. Map from `target_entity_id` (which is `recording_data.id`).
    - `kb_instrument_id`: The instrument name is in the `attributes_array`. We must UNNEST this array and look up the `kb_id` for each instrument name in our `kb_Instrument` table.
  - **Example**: For the row where AFI is the artist, a relation might show Jade Puget (`target_entity_id` from a different relation) playing 'guitar' (`attributes_array`) on the recording 'Miss Murder' (`target_entity_id` on this relation). This would create one entry in `rel_Artist_Plays_Instrument`.

- **`rel_Artist_Performed_Song` (Artist Performance on Recording)**

  - **Source**: `mb_relations_enhanced` AND `fact_plays`.
  - **Logic**: We will populate this from two sources to get a complete picture.
    1.  **MusicBrainz Catalog**: Filter `mb_relations_enhanced` for `relation_type = 'performer'` and `target_type = 'recording'`. This gives us a broad catalog of an artist's work.
    2.  **KEXP Plays**: For every row in `fact_plays`, we create a link. This confirms not only that an artist performed a song, but that it was played on KEXP.
  - **Mapping**:
    - `kb_artist_id`: Map from `artist_mb_id` (MB) or via bridges from `dim_artists_master` (KEXP).
    - `kb_song_id`: Map from `target_entity_id` (MB `recording_data.id`) or via bridges from `dim_tracks` (KEXP).

- **`rel_Artist_Person_Role_Played_Role` (Production/Creative Credits)**

  - **Source**: `mb_relations_enhanced`
  - **Filter**: `WHERE relation_type IN ('producer', 'composer', 'engineer', 'arranger', 'lyricist', 'writer') AND target_type IN ('recording', 'release')`
  - **Logic**: This is a multi-step process to capture who did what on a specific work.
    1.  **Identify the Contributor (`kb_person_id`):** The `artist_mb_id` is the person performing the role. We find their `kb_id` in `kb_Person`.
    2.  **Identify the Role (`kb_role_id`):** The `relation_type` (e.g., 'producer') is the role name. We find its `kb_id` in `kb_Role`.
    3.  **Create the Contribution Instance:** Create a new row in `kb_Artist_Person_Role` to represent this specific contribution instance, getting a new `kb_id` for it.
    4.  **Link to the Work:** The `target_entity_id` points to the recording (`kb_Song`) or release (`kb_Release`). We link the `kb_id` from step 3 to the `kb_id` of the target work in `rel_Artist_Person_Role_Played_Role`.
  - **Example**: A row where `artist_name` is "Brad Laner", `relation_type` is "producer", and the target is a recording by "Medicine" would result in linking the "Brad Laner" person, the "Producer" role, and the "Medicine" song in our KB.

- **`rel_Entity_Has_URL` (External Links)**

  - **Source**: `mb_relations_enhanced`
  - **Filter**: `WHERE target_type = 'url'`
  - **Logic**: This links entities to external resources like Discogs, Bandcamp, etc.
  - **Mapping**:
    - `kb_entity_id`: The `kb_id` of the artist from `artist_mb_id`.
    - `kb_entity_type`: 'ARTIST'.
    - `kb_url_id`: We will `UPSERT` into `kb_URL` using the `url_data.resource` as the unique address, getting a `kb_id` in return. The `kb_link_type` ENUM will be set based on the `relation_type` (e.g., 'discogs', 'bandcamp').
  - **Example**: The row for artist "Medicine" with `relation_type = 'discogs'` and `target_type = 'url'` would create a new entry in `kb_URL` and a corresponding link in `rel_Entity_Has_URL`.

- **`rel_Song_Based_On_Work` (Composition Link)**
  - **Status**: **Deferred**. As per our plan, we are not creating a `kb_Work` entity yet. This relationship will be implemented if/when that entity is added to the schema.
