'use client';

import { Input } from './ui';

interface DatePickerProps {
  value: string;
  onChange: (date: string) => void;
  label?: string;
}

export function DatePicker({ value, onChange, label }: DatePickerProps) {
  return (
    <div className="w-full sm:w-auto">
      {label && (
        <label className="block text-sm font-medium text-gray-700 mb-1.5">
          {label}
        </label>
      )}
      <Input
        type="date"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="w-full sm:w-44"
      />
    </div>
  );
}
