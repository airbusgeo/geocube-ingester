SET client_encoding = 'UTF8';
SELECT pg_catalog.set_config('search_path', '', false);


SET default_tablespace = '';
--SET default_table_access_method = heap;

CREATE TABLE public.aoi (
    id text NOT NULL,
    status text NOT NULL DEFAULT 'NEW',
    UNIQUE (id)
);

CREATE TABLE public.scene (
    id integer NOT NULL,
    aoi_id text NOT NULL,
    status text NOT NULL,
    message text NOT NULL DEFAULT '',
    source_id text NOT NULL,
    data jsonb,
    retry_countdown int NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    FOREIGN KEY (aoi_id) REFERENCES public.aoi(id) ON DELETE CASCADE
);
CREATE INDEX idx_scene_aoi ON public.scene (aoi_id);

CREATE SEQUENCE public.scene_nid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.scene_nid_seq OWNED BY public.scene.id;
ALTER TABLE ONLY public.scene ALTER COLUMN id SET DEFAULT nextval('public.scene_nid_seq'::regclass);

CREATE TABLE public.tile (
    id integer NOT NULL,
    scene_id integer NOT NULL,
    status text NOT NULL,
    message text NOT NULL DEFAULT '',
    prev integer,
    ref integer,
    source_id text NOT NULL,
    data jsonb,
    retry_countdown int NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE (source_id, scene_id),
    FOREIGN KEY (prev) REFERENCES public.tile(id),
    FOREIGN KEY (ref) REFERENCES public.tile(id),
    FOREIGN KEY (scene_id) REFERENCES public.scene(id) ON DELETE CASCADE
);
CREATE INDEX idx_tile_scene ON public.tile (scene_id);
CREATE INDEX idx_tile_prev ON public.tile (prev);
CREATE INDEX idx_tile_ref ON public.tile (ref);

CREATE SEQUENCE public.tile_nid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
ALTER SEQUENCE public.tile_nid_seq OWNED BY public.tile.id;
ALTER TABLE ONLY public.tile ALTER COLUMN id SET DEFAULT nextval('public.tile_nid_seq'::regclass);


--GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.scene TO ingester;
--GRANT SELECT,UPDATE ON SEQUENCE public.scene_nid_seq TO ingester;

--GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.tile TO ingester;
--GRANT SELECT,UPDATE ON SEQUENCE public.tile_nid_seq TO ingester;

--ALTER TABLE public.aoi OWNER TO postgres;
--GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.aoi TO ingester;
