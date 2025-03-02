import React from 'react';
import {
  ClickEvent,
  Menu,
  MenuProps,
  MenuHeader,
  SubMenu as LibSubmenu,
  MenuItem as LibMenuItem,
  MenuButton as LibMenuButton,
  FocusableItem as LibFocusableItem,
} from '@szhsin/react-menu';

import cx from 'classnames';
import styles from './Dropdown.module.scss';

export interface DropdownProps {
  id?: string;

  /** Whether the button is disabled or not */
  disabled?: boolean;
  ['data-testid']?: string;
  className?: string;
  menuButtonClassName?: string;

  /** Dropdown label */
  label: string;

  /** Dropdown value*/
  value?: string;

  children?: JSX.Element[] | JSX.Element;

  /** Event that fires when an item is activated*/
  onItemClick?: (event: ClickEvent) => void;

  overflow?: MenuProps['overflow'];
  position?: MenuProps['position'];

  menuButton?: JSX.Element;
}

export default function Dropdown({
  id,
  children,
  className,
  disabled,
  value,
  label,
  onItemClick,
  overflow,
  position,
  menuButtonClassName = '',
  ...props
}: DropdownProps) {
  const menuButtonComponent = props.menuButton || (
    <MenuButton
      className={`${styles.dropdownMenuButton} ${menuButtonClassName}`}
      disabled={disabled}
      type="button"
    >
      {value || label}
    </MenuButton>
  );

  return (
    <Menu
      id={id}
      className={cx(className, styles.dropdownMenu)}
      data-testid={props['data-testid']}
      onItemClick={onItemClick}
      overflow={overflow}
      position={position}
      menuButton={menuButtonComponent}
    >
      <MenuHeader>{label}</MenuHeader>
      {children}
    </Menu>
  );
}

export const SubMenu = LibSubmenu;
export const MenuItem = LibMenuItem;
export const MenuButton = LibMenuButton as ShamefulAny;
export const FocusableItem = LibFocusableItem;
