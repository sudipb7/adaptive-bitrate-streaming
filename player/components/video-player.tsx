"use client";

import {
  Pause,
  Play,
  Volume2,
  VolumeX,
  Maximize2,
  Minimize2,
  Settings,
  SkipBack,
  SkipForward,
  Check,
} from "lucide-react";
import Hls from "hls.js";
import { useEffect, useRef, useState, useCallback } from "react";

import { Button } from "@/components/ui/button";
import { Slider } from "@/components/ui/slider";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/ui/hover-card";
import { cn } from "@/lib/utils";

interface Quality {
  height: number;
  bitrate: number;
}

interface VideoPlayerProps {
  src: string;
  title?: string;
}

export default function VideoPlayer({ src, title = "HLS Adaptive Bitrate Streaming Platform" }: VideoPlayerProps) {
  const videoRef = useRef<HTMLVideoElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const progressRef = useRef<HTMLDivElement>(null);
  const hlsRef = useRef<Hls | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isMuted, setIsMuted] = useState(false);
  const [volume, setVolume] = useState(1);
  const [qualities, setQualities] = useState<Quality[]>([]);
  const [currentQuality, setCurrentQuality] = useState<number | "auto">("auto");
  const [currentTime, setCurrentTime] = useState(0);
  const [duration, setDuration] = useState(0);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [showControls, setShowControls] = useState(true);
  const [isBuffering, setIsBuffering] = useState(false);

  const hideControlsTimer = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
    }
    timerRef.current = setTimeout(() => {
      if (isPlaying) {
        setShowControls(false);
        if (containerRef.current) {
          containerRef.current.style.cursor = "none";
        }
      }
    }, 3000);
  }, [isPlaying]);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    if (Hls.isSupported()) {
      const hls = new Hls();
      hlsRef.current = hls;
      hls.loadSource(src);
      hls.attachMedia(video);

      hls.on(Hls.Events.MANIFEST_PARSED, () => {
        const levels = hls.levels.map((level) => ({
          height: level.height,
          bitrate: level.bitrate,
        }));
        setQualities(levels);
      });

      hls.on(Hls.Events.LEVEL_SWITCHED, (_, data) => {
        setCurrentQuality(data.level);
      });
    } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
      video.src = src;
    }

    const updateTime = () => setCurrentTime(video.currentTime);
    const handleLoadedMetadata = () => setDuration(video.duration);
    const handlePlay = () => setIsPlaying(true);
    const handlePause = () => setIsPlaying(false);
    const handleWaiting = () => setIsBuffering(true);
    const handleCanPlay = () => setIsBuffering(false);

    video.addEventListener("timeupdate", updateTime);
    video.addEventListener("loadedmetadata", handleLoadedMetadata);
    video.addEventListener("play", handlePlay);
    video.addEventListener("pause", handlePause);
    video.addEventListener("waiting", handleWaiting);
    video.addEventListener("canplay", handleCanPlay);

    return () => {
      video.removeEventListener("timeupdate", updateTime);
      video.removeEventListener("loadedmetadata", handleLoadedMetadata);
      video.removeEventListener("play", handlePlay);
      video.removeEventListener("pause", handlePause);
      video.removeEventListener("waiting", handleWaiting);
      video.removeEventListener("canplay", handleCanPlay);
      if (hlsRef.current) {
        hlsRef.current.destroy();
      }
      if (timerRef.current) {
        clearTimeout(timerRef.current);
      }
    };
  }, [src]);

  const togglePlay = () => {
    if (videoRef.current) {
      if (isPlaying) {
        videoRef.current.pause();
      } else {
        videoRef.current.play();
      }
    }
  };

  const toggleMute = () => {
    if (videoRef.current) {
      videoRef.current.muted = !isMuted;
      setIsMuted(!isMuted);
    }
  };

  const changeVolume = (value: number[]) => {
    const newVolume = value[0];
    if (videoRef.current) {
      videoRef.current.volume = newVolume;
      setVolume(newVolume);
      setIsMuted(newVolume === 0);
    }
  };

  const changeQuality = (quality: number | "auto") => {
    if (hlsRef.current) {
      if (quality === "auto") {
        hlsRef.current.currentLevel = -1; // -1 means auto
      } else {
        hlsRef.current.currentLevel = quality;
      }
      setCurrentQuality(quality);
    }
  };

  const formatTime = (time: number) => {
    const minutes = Math.floor(time / 60);
    const seconds = Math.floor(time % 60);
    return `${minutes}:${seconds < 10 ? "0" : ""}${seconds}`;
  };

  const seekTo = (value: number[]) => {
    const time = value[0];
    if (videoRef.current) {
      videoRef.current.currentTime = time;
      setCurrentTime(time);
    }
  };

  const skipForward = () => {
    if (videoRef.current) {
      videoRef.current.currentTime += 10;
    }
  };

  const skipBackward = () => {
    if (videoRef.current) {
      videoRef.current.currentTime -= 10;
    }
  };

  const toggleFullscreen = () => {
    if (!document.fullscreenElement) {
      containerRef.current?.requestFullscreen();
      setIsFullscreen(true);
    } else {
      document.exitFullscreen();
      setIsFullscreen(false);
    }
  };

  const handleMouseMove = () => {
    setShowControls(true);
    if (containerRef.current) {
      containerRef.current.style.cursor = "default";
    }
    hideControlsTimer();
  };

  return (
    <div
      ref={containerRef}
      className="relative rounded-lg overflow-hidden shadow-lg h-[500px] aspect-video mx-auto bg-black/40 group"
      onMouseMove={handleMouseMove}
      onMouseLeave={() => setShowControls(false)}
    >
      <video ref={videoRef} className="w-full h-full cursor-pointer" onClick={togglePlay} />
      {isBuffering && (
        <div className="absolute inset-0 flex items-center justify-center">
          <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-white"></div>
        </div>
      )}
      <div
        className={cn(
          "absolute inset-0 bg-gradient-to-t from-black/40 to-transparent transition-opacity duration-300",
          showControls ? "opacity-100" : "opacity-0"
        )}
      >
        <div className="absolute top-4 left-4 text-white font-semibold cursor-text">{title}</div>
        <div className="absolute bottom-0 left-0 right-0 p-2.5">
          <div
            ref={progressRef}
            className="w-full h-1 bg-gray-600 hover:scale-y-105 rounded-full overflow-hidden cursor-pointer"
            onClick={(e) => {
              const rect = progressRef.current?.getBoundingClientRect();
              if (rect) {
                const percent = (e.clientX - rect.left) / rect.width;
                seekTo([percent * duration]);
              }
            }}
          >
            <div
              className="h-full bg-red-600 transition-all duration-100 ease-linear"
              style={{ width: `${(currentTime / duration) * 100}%` }}
            ></div>
          </div>
          <div className="flex items-center justify-between mt-2">
            <div className="flex items-center space-x-2">
              <Button variant="ghost" size="icon" onClick={togglePlay} className="text-white">
                {isPlaying ? <Pause className="h-4 w-4" /> : <Play className="h-4 w-4" />}
              </Button>
              <Button variant="ghost" size="icon" onClick={skipBackward} className="text-white">
                <SkipBack className="h-4 w-4" />
              </Button>
              <Button variant="ghost" size="icon" onClick={skipForward} className="text-white">
                <SkipForward className="h-4 w-4" />
              </Button>
              <HoverCard openDelay={150}>
                <HoverCardTrigger asChild>
                  <Button variant="ghost" size="icon" onClick={toggleMute} className="text-white">
                    {isMuted ? <VolumeX className="h-4 w-4" /> : <Volume2 className="h-4 w-4" />}
                  </Button>
                </HoverCardTrigger>
                <HoverCardContent side="top" className="w-40 p-2">
                  <Slider
                    value={[volume]}
                    max={1}
                    step={0.01}
                    onValueChange={changeVolume}
                    className="w-full cursor-pointer"
                  />
                </HoverCardContent>
              </HoverCard>
              <span className="text-sm text-white">
                {formatTime(currentTime)} / {formatTime(duration)}
              </span>
            </div>
            <div className="flex items-center space-x-2">
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button variant="ghost" size="icon" className="text-white">
                    <Settings className="h-4 w-4" />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end" side="top">
                  <DropdownMenuLabel>Quality</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem onClick={() => changeQuality("auto")}>
                    Auto {currentQuality === "auto" && <Check className="ml-2 h-4 w-4" />}
                  </DropdownMenuItem>
                  {qualities.map((quality, index) => (
                    <DropdownMenuItem key={index} onClick={() => changeQuality(index)}>
                      {quality.height}p {currentQuality === index && <Check className="ml-2 h-4 w-4" />}
                    </DropdownMenuItem>
                  ))}
                </DropdownMenuContent>
              </DropdownMenu>
              <Button variant="ghost" size="icon" onClick={toggleFullscreen} className="text-white">
                {isFullscreen ? <Minimize2 className="h-4 w-4" /> : <Maximize2 className="h-4 w-4" />}
              </Button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
