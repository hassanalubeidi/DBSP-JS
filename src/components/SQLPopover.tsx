import { useState, useEffect, useRef, useCallback } from 'react';
import { createPortal } from 'react-dom';

interface SQLPopoverProps {
  sql: string;
}

export function SQLPopover({ sql }: SQLPopoverProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [position, setPosition] = useState({ top: 0, left: 0 });
  const buttonRef = useRef<HTMLButtonElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  
  // Calculate position when opening
  const updatePosition = useCallback(() => {
    if (buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      const popoverWidth = 380;
      const popoverHeight = 300; // estimated max height
      
      let left = rect.left;
      let top = rect.bottom + 8;
      
      // Adjust if would overflow right edge
      if (left + popoverWidth > window.innerWidth - 16) {
        left = window.innerWidth - popoverWidth - 16;
      }
      
      // Adjust if would overflow bottom edge - show above instead
      if (top + popoverHeight > window.innerHeight - 16) {
        top = rect.top - popoverHeight - 8;
        if (top < 16) top = 16; // Don't go above viewport
      }
      
      // Don't go off left edge
      if (left < 16) left = 16;
      
      setPosition({ top, left });
    }
  }, []);
  
  // Update position on open and scroll/resize
  useEffect(() => {
    if (isOpen) {
      updatePosition();
      window.addEventListener('scroll', updatePosition, true);
      window.addEventListener('resize', updatePosition);
      return () => {
        window.removeEventListener('scroll', updatePosition, true);
        window.removeEventListener('resize', updatePosition);
      };
    }
  }, [isOpen, updatePosition]);
  
  // Close on click outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        popoverRef.current && 
        !popoverRef.current.contains(e.target as Node) &&
        buttonRef.current &&
        !buttonRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [isOpen]);
  
  // Close on escape
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setIsOpen(false);
    };
    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      return () => document.removeEventListener('keydown', handleEscape);
    }
  }, [isOpen]);
  
  return (
    <>
      <button 
        ref={buttonRef}
        className="sql-icon-btn" 
        onClick={() => setIsOpen(!isOpen)}
        title="View SQL"
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
          <polyline points="14 2 14 8 20 8"/>
          <line x1="16" y1="13" x2="8" y2="13"/>
          <line x1="16" y1="17" x2="8" y2="17"/>
          <polyline points="10 9 9 9 8 9"/>
        </svg>
      </button>
      {isOpen && createPortal(
        <div 
          ref={popoverRef}
          className="sql-popover-portal"
          style={{ 
            top: position.top, 
            left: position.left,
          }}
        >
          <div className="sql-popover-header">
            <span>SQL Query</span>
            <button className="sql-popover-close" onClick={() => setIsOpen(false)}>×</button>
          </div>
          <pre className="sql-popover-code">{sql}</pre>
        </div>,
        document.body
      )}
    </>
  );
}
